// Package raft implements the Raft consensus algorithm (leader election,
// heartbeats, log replication and commit management) as described in the
// extended Raft paper. Persistence, membership changes and snapshots are out
// of scope for Lab 2.
//
// Public interface (kept as required by the lab):
//
//	rf := Make(peers, me, applyCh)
//	index, term, isLeader := rf.Start(command)
//	term, isLeader := rf.GetState()
//	ApplyMsg delivered on applyCh for every committed entry, in log order.
//
// Transport: peers are *labrpc.Client values (TCP + JSON). Handlers therefore
// take their arguments by value and return (reply, error). Because JSON would
// turn an int command into a float64 on the receiving peer, commands travel
// as gob-encoded bytes (EntryWire.Data) so every peer applies the same Go
// type. Command types other than the basic ones must be gob.Register'ed.
//
// Locking rule: rf.mu protects all mutable state. It is never held across an
// RPC (peer.Call) or across a send on applyCh.
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"lab2/labrpc"
)

// Timing parameters.
//
// Heartbeats are sent at most once per heartbeatInterval (~9/s, i.e. fewer
// than the 10/s limit in the assignment). Election timeouts are randomized in
// [electionTimeoutMin, electionTimeoutMax); the ratio to the heartbeat
// interval keeps a couple of lost heartbeats from triggering an election, and
// a new leader is still elected in well under the 5 s allowed.
const (
	heartbeatInterval  = 110 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	tickerInterval     = 10 * time.Millisecond
)

// ApplyMsg is sent on applyCh for each newly committed log entry.
// UseSnapshot and Snapshot exist only for interface compatibility and are
// always false / nil in this lab.
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// LogEntry is one entry of the replicated log. Command may be any
// gob-encodable value (types other than the basic ones must be registered
// with gob.Register by the service).
type LogEntry struct {
	Term    int
	Command interface{}
	data    []byte // gob encoding of Command, kept so it can be (re)sent as-is
}

// EntryWire is the on-the-wire form of a log entry.
type EntryWire struct {
	Term int
	Data []byte
}

func encodeCommand(cmd interface{}) ([]byte, error) {
	if cmd == nil {
		return nil, nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var cmd interface{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string {
	switch r {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	default:
		return "leader"
	}
}

// RequestVoteArgs is the RequestVote RPC argument (Figure 2).
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply is the RequestVote RPC reply.
type RequestVoteReply struct {
	Term        int  // receiver's currentTerm, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// AppendEntriesArgs is the AppendEntries RPC argument (Figure 2). With an
// empty Entries slice it is a heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []EntryWire
	LeaderCommit int
}

// AppendEntriesReply is the AppendEntries RPC reply. ConflictTerm and
// ConflictIndex implement the optional fast log-backup optimization from
// Section 5.3 of the paper, so a leader can repair a divergent follower with
// roughly one RPC per conflicting term rather than one per entry.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// Valid only when Success is false because of a log mismatch.
	// ConflictTerm == -1 means the follower's log is too short, in which case
	// ConflictIndex is the follower's log length (next free index).
	ConflictTerm  int
	ConflictIndex int
}

// Raft is a single Raft peer.
type Raft struct {
	mu      sync.Mutex
	peers   []*labrpc.Client
	me      int
	applyCh chan ApplyMsg

	killed int32
	killCh chan struct{}

	// Persistent state (kept in memory only; persistence is out of scope).
	currentTerm int
	votedFor    int        // -1 if none
	log         []LogEntry // log[0] is a sentinel; first real entry is index 1

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	state       role

	// Volatile state on leaders (reinitialized after each election).
	nextIndex  []int
	matchIndex []int

	electionDeadline time.Time
	lastBroadcast    time.Time
	applyCond        *sync.Cond
}

// Make creates a Raft peer and starts its background goroutines. It returns
// quickly. peers[me] is this peer's own end-point and is never used for RPCs.
func Make(peers []*labrpc.Client, me int, applyCh chan ApplyMsg) *Raft {
	rf := newRaft(peers, me, applyCh)
	go rf.ticker()
	go rf.applier()
	return rf
}

// newRaft builds the peer without starting goroutines (used by unit tests).
func newRaft(peers []*labrpc.Client, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		me:         me,
		applyCh:    applyCh,
		killCh:     make(chan struct{}),
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		state:      follower,
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimerLocked()
	return rf
}

// Kill stops all background goroutines of this peer. It is not part of the
// required interface; the test harness here uses it for clean shutdown.
func (rf *Raft) Kill() {
	if atomic.CompareAndSwapInt32(&rf.killed, 0, 1) {
		close(rf.killCh)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

func (rf *Raft) isKilled() bool { return atomic.LoadInt32(&rf.killed) == 1 }

// GetState returns the current term and whether this peer believes it is the
// leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// Start begins agreement on command. If this peer is not the leader it
// returns (-1, currentTerm, false). Otherwise it appends the command to the
// leader's log, triggers replication, and returns immediately with the index
// the command will have if it is ever committed.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// A command that cannot be gob-encoded (e.g. an unregistered custom type)
	// cannot be replicated, so it is rejected like a non-leader call.
	data, err := encodeCommand(command)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isKilled() || rf.state != leader || err != nil {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command, data: data})
	index := rf.lastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.advanceCommitLocked() // needed for single-peer clusters
	rf.broadcastLocked()
	return index, rf.currentTerm, true
}

// Small helpers (caller must hold rf.mu)
func (rf *Raft) lastLogIndex() int { return len(rf.log) - 1 }
func (rf *Raft) lastLogTerm() int  { return rf.log[len(rf.log)-1].Term }

func (rf *Raft) resetElectionTimerLocked() {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(span))
	rf.electionDeadline = time.Now().Add(timeout)
}

// stepDownLocked moves to term (> currentTerm) as a follower with no vote.
func (rf *Raft) stepDownLocked(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = follower
}

// Ticker: election timeouts and heartbeats
func (rf *Raft) ticker() {
	t := time.NewTicker(tickerInterval)
	defer t.Stop()
	for {
		select {
		case <-rf.killCh:
			return
		case <-t.C:
		}
		rf.mu.Lock()
		now := time.Now()
		switch rf.state {
		case leader:
			if now.Sub(rf.lastBroadcast) >= heartbeatInterval {
				rf.broadcastLocked()
			}
		default:
			if now.After(rf.electionDeadline) {
				rf.startElectionLocked()
			}
		}
		rf.mu.Unlock()
	}
}

// Leader election
func (rf *Raft) startElectionLocked() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1 // own vote
	if votes > len(rf.peers)/2 {
		rf.becomeLeaderLocked() // single-peer cluster
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if err := rf.peers[peer].Call("RequestVote", &reply, args); err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Ignore stale replies: we may have moved on to another term/role.
			if rf.state != candidate || rf.currentTerm != term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term)
				rf.resetElectionTimerLocked()
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeaderLocked()
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = leader
	next := rf.lastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.broadcastLocked() // initial empty AppendEntries to establish authority
}

// RequestVote is the RPC handler invoked by candidates.
func (rf *Raft) RequestVote(args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}
	reply.Term = rf.currentTerm

	// Election restriction (Section 5.4.1): the candidate's log must be at
	// least as up-to-date as ours.
	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked() // only reset when a vote is actually granted
	}
	return reply, nil
}

// Log replication: leader side

// broadcastLocked sends AppendEntries (entries or heartbeat) to every peer.
func (rf *Raft) broadcastLocked() {
	rf.lastBroadcast = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateTo(i, rf.currentTerm)
		}
	}
}

// replicateTo sends AppendEntries to one peer and processes the reply. On a
// log mismatch it backs nextIndex up and retries immediately.
func (rf *Raft) replicateTo(peer, term int) {
	for !rf.isKilled() {
		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		next := rf.nextIndex[peer]
		if next < 1 {
			next = 1
		}
		if next > rf.lastLogIndex()+1 {
			next = rf.lastLogIndex() + 1
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.log[next-1].Term,
			LeaderCommit: rf.commitIndex,
		}
		// Copy into wire form: the log may be appended to while the RPC is in flight.
		for _, e := range rf.log[next:] {
			args.Entries = append(args.Entries, EntryWire{Term: e.Term, Data: e.data})
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply
		if err := rf.peers[peer].Call("AppendEntries", &reply, args); err != nil {
			return // lost or peer unreachable; the next heartbeat retries
		}

		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.stepDownLocked(reply.Term)
			rf.resetElectionTimerLocked()
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			if match > rf.matchIndex[peer] {
				rf.matchIndex[peer] = match
			}
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitLocked()
			rf.mu.Unlock()
			return
		}

		// Mismatch. Ignore if the reply is stale (nextIndex already moved).
		if rf.nextIndex[peer] != args.PrevLogIndex+1 {
			rf.mu.Unlock()
			return
		}
		newNext := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			// If we have entries of the conflicting term, skip past our last one.
			for i := rf.lastLogIndex(); i >= 1; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					newNext = i + 1
					break
				}
				if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}
		}
		if newNext > args.PrevLogIndex { // guarantee progress
			newNext = args.PrevLogIndex
		}
		if newNext < 1 {
			newNext = 1
		}
		rf.nextIndex[peer] = newNext
		rf.mu.Unlock()
		// loop: retry with the smaller nextIndex
	}
}

// advanceCommitLocked advances commitIndex to the highest N replicated on a
// majority whose entry is from the current term (Section 5.4.2: entries from
// earlier terms are only committed indirectly).
func (rf *Raft) advanceCommitLocked() {
	if rf.state != leader {
		return
	}
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break // all earlier entries have smaller terms
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			return
		}
	}
}

// Log replication: follower side
// AppendEntries is the RPC handler invoked by leaders (also as heartbeat).
func (rf *Raft) AppendEntries(args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	// Decode commands before touching any state; a bad payload is an error.
	entries := make([]LogEntry, len(args.Entries))
	for i, w := range args.Entries {
		cmd, err := decodeCommand(w.Data)
		if err != nil {
			return reply, fmt.Errorf("decode command: %w", err)
		}
		entries[i] = LogEntry{Term: w.Term, Command: cmd, data: w.Data}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}
	// A valid leader exists for this term: candidates revert to follower and
	// every peer restarts its election timer.
	rf.state = follower
	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	// Consistency check.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return reply, nil
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && rf.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = ci
		return reply, nil
	}

	// Append new entries, truncating only on a real conflict so that delayed
	// or duplicated RPCs cannot remove entries we already hold.
	for i, e := range entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx] // conflict: drop this entry and all after it
		}
		rf.log = append(rf.log, entries[i:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(entries)
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
	reply.Success = true
	return reply, nil
}

// Applying committed entries
// applier delivers committed entries to the service in log order, exactly
// once each. Sends on applyCh happen without rf.mu held so RPC handlers and
// other goroutines keep making progress even if the service is slow.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.isKilled() {
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			continue
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Command: rf.log[i].Command})
		}
		rf.lastApplied = end // only this goroutine applies, so order is kept
		rf.mu.Unlock()
		for _, m := range msgs {
			select {
			case rf.applyCh <- m:
			case <-rf.killCh:
				rf.mu.Lock()
				return
			}
		}
		rf.mu.Lock()
	}
}
