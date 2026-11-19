package raft

import (
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lab2/labrpc"
)

// Tests whose names end in 3A cover leader election and heartbeats; names
// ending in 3B cover log replication and commitment. Run them with
//   go test -run 3A ./raft     and     go test -run 3B ./raft

const electionWait = 1000 * time.Millisecond
// 3A: leader election and heartbeats

func TestInitialElection3A(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()
	cfg.begin("Test 3A: initial election")

	cfg.checkOneLeader()

	// Terms must agree and be stable while the leader keeps sending heartbeats.
	time.Sleep(50 * time.Millisecond)
	term1 := cfg.checkTerms()
	time.Sleep(2 * electionWait)
	term2 := cfg.checkTerms()
	if term1 != term2 {
		t.Fatalf("term changed (%d -> %d) even though there was no failure", term1, term2)
	}
	cfg.checkOneLeader()
}

func TestReElection3A(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()
	cfg.begin("Test 3A: election after network failure")

	leader1 := cfg.checkOneLeader()

	// Leader disconnects: a new one must be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	// Old leader rejoins: it must not disturb the new leader.
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()

	// No quorum: no new leader may emerge.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % 3)
	time.Sleep(2 * electionWait)
	cfg.checkNoLeader()

	// Quorum restored: a leader appears.
	cfg.connect((leader2 + 1) % 3)
	cfg.checkOneLeader()

	// Everyone back.
	cfg.connect(leader2)
	cfg.checkOneLeader()
}

func TestManyElections3A(t *testing.T) {
	const servers = 7
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3A: multiple elections")

	cfg.checkOneLeader()
	for i := 0; i < 10; i++ {
		i1, i2, i3 := rand.Int()%servers, rand.Int()%servers, rand.Int()%servers
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)
		cfg.checkOneLeader() // 4 of 7 remain
		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}
	cfg.checkOneLeader()
}

func TestHeartbeatRate3A(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()
	cfg.begin("Test 3A: heartbeat rate")

	cfg.checkOneLeader()
	time.Sleep(200 * time.Millisecond)
	before := cfg.rpcTotal()
	time.Sleep(3 * time.Second)
	rate := float64(cfg.rpcTotal()-before) / 3.0 / 2.0 // per second, per follower
	if rate > 10.0 {
		t.Fatalf("leader sends %.1f heartbeats/s per follower, limit is 10", rate)
	}
	t.Logf("heartbeat rate: %.1f/s per follower", rate)
}

func TestRequestVoteRules3A(t *testing.T) {
	rf := newRaft(make([]*labrpc.Client, 3), 0, make(chan ApplyMsg))
	rf.currentTerm = 3
	rf.log = []LogEntry{le(0, nil), le(1, "a"), le(2, "b"), le(2, "c")} // last index 3, last term 2

	vote := func(term, cand, lastIdx, lastTerm int) RequestVoteReply {
		r, _ := rf.RequestVote(RequestVoteArgs{Term: term, CandidateId: cand, LastLogIndex: lastIdx, LastLogTerm: lastTerm})
		return r
	}

	if r := vote(2, 1, 9, 9); r.VoteGranted || r.Term != 3 {
		t.Fatalf("stale-term candidate must be rejected and told our term, got %+v", r)
	}
	if r := vote(4, 1, 2, 2); r.VoteGranted || r.Term != 4 {
		t.Fatalf("shorter log (same last term) must be rejected, got %+v", r)
	}
	if r := vote(4, 1, 9, 1); r.VoteGranted {
		t.Fatalf("older last term must be rejected even with a longer log, got %+v", r)
	}
	if r := vote(4, 1, 3, 2); !r.VoteGranted {
		t.Fatalf("equally up-to-date candidate should get the vote, got %+v", r)
	}
	if r := vote(4, 2, 5, 5); r.VoteGranted {
		t.Fatalf("second candidate in the same term must be rejected, got %+v", r)
	}
	if r := vote(4, 1, 3, 2); !r.VoteGranted {
		t.Fatalf("repeat request from the same candidate should be granted, got %+v", r)
	}
	if r := vote(5, 2, 1, 3); !r.VoteGranted { // higher last term wins despite shorter log
		t.Fatalf("higher last term should win the vote in a new term, got %+v", r)
	}
}

func TestStepDownOnHigherTerm3A(t *testing.T) {
	rf := newRaft(make([]*labrpc.Client, 3), 0, make(chan ApplyMsg))
	rf.mu.Lock()
	rf.currentTerm, rf.state, rf.votedFor = 5, leader, 0
	rf.mu.Unlock()
	rep, _ := rf.AppendEntries(AppendEntriesArgs{Term: 4, LeaderId: 1})
	if rep.Success || rep.Term != 5 || rf.state != leader {
		t.Fatalf("stale leader's AppendEntries must be rejected without changing role: %+v", rep)
	}
	rep, _ = rf.AppendEntries(AppendEntriesArgs{Term: 6, LeaderId: 1})
	if !rep.Success || rf.state != follower || rf.currentTerm != 6 || rf.votedFor != -1 {
		t.Fatalf("higher-term AppendEntries must demote to follower: %+v state=%v term=%d", rep, rf.state, rf.currentTerm)
	}
}

// 3B: log replication, commitment, recovery

func TestBasicAgree3B(t *testing.T) {
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: basic agreement")

	for index := 1; index < 4; index++ {
		if nd, _ := cfg.nCommitted(index); nd > 0 {
			t.Fatalf("some have committed before Start()")
		}
		if xindex := cfg.one(index*100, servers, false); xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}
}

func TestSinglePeer3B(t *testing.T) {
	cfg := makeConfig(t, 1, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: single-peer cluster")
	cfg.checkOneLeader()
	cfg.one(1, 1, false)
	cfg.one(2, 1, false)
}

func TestFailAgree3B(t *testing.T) {
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: agreement despite one disconnected server")

	cfg.one(101, servers, false)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)

	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(electionWait)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// The lagging follower catches up after reconnecting.
	cfg.connect((leader + 1) % servers)
	cfg.one(106, servers, true)
	time.Sleep(electionWait)
	cfg.one(107, servers, true)
}

func TestFailNoAgree3B(t *testing.T) {
	const servers = 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: no agreement if too many followers disconnect")

	cfg.one(10, servers, false)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.rafts[leader].Start(20)
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}
	time.Sleep(2 * electionWait)
	if n, _ := cfg.nCommitted(index); n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// The old leader may or may not keep index 2 (a new leader may have been
	// elected), but the cluster must be fully functional again.
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if !ok2 {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}
	cfg.one(1000, servers, true)
}

func TestConcurrentStarts3B(t *testing.T) {
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := cfg.checkOneLeader()
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			continue
		}

		var wg sync.WaitGroup
		iters := 5
		indexes := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := cfg.rafts[leader].Start(100 + i)
				if term1 != term || !ok {
					return
				}
				indexes <- idx
			}(ii)
		}
		wg.Wait()
		close(indexes)

		for j := 0; j < servers; j++ {
			if t2, _ := cfg.rafts[j].GetState(); t2 != term {
				continue loop // term changed: retry
			}
		}

		var cmds []int
		for index := range indexes {
			cmd := cfg.waitCommitted(index, servers, term)
			if ci, ok := cmd.(int); ok {
				cmds = append(cmds, ci)
			} else if cmd == nil {
				continue loop
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}
		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			found := false
			for _, c := range cmds {
				if c == x {
					found = true
				}
			}
			if !found {
				t.Fatalf("cmd %d missing in %v", x, cmds)
			}
		}
		success = true
		break
	}
	if !success {
		t.Fatalf("term changed too often")
	}
}

// waitCommitted waits for at least n servers to commit index and returns the
// command, or nil if a term change makes agreement impossible to observe.
func (cfg *config) waitCommitted(index, n, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, rf := range cfg.rafts {
				if t, _ := rf.GetState(); t > startTerm {
					return nil
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d", nd, index, n)
	}
	return cmd
}

func TestRejoin3B(t *testing.T) {
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: rejoin of partitioned leader")

	cfg.one(101, servers, true)

	// Leader network failure.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// The old leader accepts entries that can never commit.
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// The new leader commits at the same indexes.
	cfg.one(103, 2, true)

	// New leader fails; the old leader returns.
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.connect(leader1)

	cfg.one(104, 2, true)

	// Everyone back.
	cfg.connect(leader2)
	cfg.one(105, servers, true)
}

func TestBackup3B(t *testing.T) {
	const servers = 5
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// Put the leader and one follower in a partition.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// Lots of commands that will never commit.
	for i := 0; i < 50; i++ {
		cfg.rafts[leader1].Start(rand.Int())
	}
	time.Sleep(electionWait / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// The other partition recovers.
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// Lots of successful commands to the new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// Another partitioned leader and one follower.
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// Lots more commands that won't commit.
	for i := 0; i < 50; i++ {
		cfg.rafts[leader2].Start(rand.Int())
	}
	time.Sleep(electionWait / 2)

	// Bring the original leader back, with `other`, and one more server.
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// Lots of successful commands to the new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// Now everyone.
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)
}

func TestCount3B(t *testing.T) {
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: RPC counts aren't too high")

	leader := cfg.checkOneLeader()
	total1 := cfg.rpcTotal()
	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader = cfg.checkOneLeader()
		total1 = cfg.rpcTotal()

		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				continue loop
			}
			if !ok {
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}
		for i := 1; i < iters+1; i++ {
			cmd := cfg.waitCommitted(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t2, _ := cfg.rafts[j].GetState(); t2 != term {
				failed = true
			}
		}
		total2 = cfg.rpcTotal()
		if failed {
			continue loop
		}
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries", total2-total1, iters)
		}
		success = true
		break
	}
	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(electionWait)
	total3 := cfg.rpcTotal()
	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness", total3-total2)
	}
}

func TestUnreliableAgree3B(t *testing.T) {
	const servers = 5
	cfg := makeConfig(t, servers, true)
	defer cfg.cleanup()
	cfg.begin("Test 3B: unreliable agreement")

	var wg sync.WaitGroup
	for iters := 1; iters < 30; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		cfg.one(iters, 1, true)
	}
	cfg.setReliable(true)
	wg.Wait()
	cfg.one(100, servers, true)
}

// TestUnreliableChurn3B keeps starting commands while servers are repeatedly
// disconnected/reconnected on an unreliable network, then checks that after
// healing every server converges on the same committed log. The harness
// itself fails the test if two servers ever apply different commands at the
// same index, or apply an index twice / out of order.
func TestUnreliableChurn3B(t *testing.T) {
	const servers = 5
	cfg := makeConfig(t, servers, true)
	defer cfg.cleanup()
	cfg.begin("Test 3B: churn on an unreliable network")

	stop := int32(0)
	var wg sync.WaitGroup
	for c := 0; c < 3; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for atomic.LoadInt32(&stop) == 0 {
				for s := 0; s < servers; s++ {
					cfg.mu.Lock()
					rf := cfg.rafts[s]
					cfg.mu.Unlock()
					rf.Start(rand.Int())
				}
				time.Sleep(time.Duration(10+rand.Intn(40)) * time.Millisecond)
			}
		}(c)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		i := rand.Intn(servers)
		if rand.Intn(2) == 0 {
			cfg.disconnect(i)
		} else {
			cfg.connect(i)
		}
		time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.setReliable(true)
	time.Sleep(electionWait)
	last := cfg.one(rand.Int(), servers, true)
	// every server has applied the same prefix through `last`
	for i := 1; i <= last; i++ {
		cfg.waitCommitted(i, servers, -1)
	}
	cfg.checkApplyErrors()
}

type kvCommand struct {
	Key   string
	Value int
}

func TestNonIntegerCommands3B(t *testing.T) {
	gob.Register(kvCommand{})
	const servers = 3
	cfg := makeConfig(t, servers, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: commands are not assumed to be integers")

	cfg.one(kvCommand{"a", 1}, servers, false)
	cfg.one("a string command", servers, false)
	cfg.one(kvCommand{"b", 2}, servers, false)
	cfg.one(3.5, servers, false)
}

func TestStartOnFollowerRejected3B(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()
	cfg.begin("Test 3B: Start() on a follower")
	leader := cfg.checkOneLeader()
	f := (leader + 1) % 3
	if idx, _, ok := cfg.rafts[f].Start(42); ok || idx != -1 {
		t.Fatalf("follower accepted Start(): idx=%d ok=%v", idx, ok)
	}
}

// 3B: AppendEntries handler and commit-rule unit tests
func le(term int, cmd interface{}) LogEntry {
	d, err := encodeCommand(cmd)
	if err != nil {
		panic(err)
	}
	return LogEntry{Term: term, Command: cmd, data: d}
}

func we(term int, cmd interface{}) EntryWire {
	d, err := encodeCommand(cmd)
	if err != nil {
		panic(err)
	}
	return EntryWire{Term: term, Data: d}
}

func newTestRaft() *Raft {
	return newRaft(make([]*labrpc.Client, 3), 0, make(chan ApplyMsg))
}

func TestAppendEntriesRules3B(t *testing.T) {
	rf := newTestRaft()
	rf.currentTerm = 2
	rf.log = []LogEntry{le(0, nil), le(1, "a"), le(1, "b"), le(2, "c")}

	ae := func(args AppendEntriesArgs) AppendEntriesReply {
		r, err := rf.AppendEntries(args)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// Stale term.
	if r := ae(AppendEntriesArgs{Term: 1}); r.Success || r.Term != 2 {
		t.Fatalf("stale term accepted: %+v", r)
	}
	// prev beyond the end of our log: too short.
	if r := ae(AppendEntriesArgs{Term: 2, PrevLogIndex: 6, PrevLogTerm: 2}); r.Success || r.ConflictTerm != -1 || r.ConflictIndex != 4 {
		t.Fatalf("short-log conflict info wrong: %+v", r)
	}
	// prev term mismatch: report term and first index of that term.
	if r := ae(AppendEntriesArgs{Term: 2, PrevLogIndex: 2, PrevLogTerm: 2}); r.Success || r.ConflictTerm != 1 || r.ConflictIndex != 1 {
		t.Fatalf("term-mismatch conflict info wrong: %+v", r)
	}
	// A delayed RPC carrying a prefix must not truncate matching entries.
	if r := ae(AppendEntriesArgs{Term: 2, PrevLogIndex: 0, Entries: []EntryWire{we(1, "a")}}); !r.Success {
		t.Fatalf("prefix append failed: %+v", r)
	}
	if len(rf.log) != 4 {
		t.Fatalf("matching prefix truncated the log: len=%d", len(rf.log))
	}
	// Real conflict at index 3 (term 2 vs 3): delete it and everything after.
	r := ae(AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []EntryWire{we(3, "x"), we(3, "y")}, LeaderCommit: 9})
	if !r.Success || len(rf.log) != 5 || rf.log[3].Command != "x" || rf.log[4].Command != "y" {
		t.Fatalf("conflicting suffix not replaced: %+v log=%v", r, rf.log)
	}
	// commitIndex = min(leaderCommit, index of last new entry) = 4.
	if rf.commitIndex != 4 {
		t.Fatalf("commitIndex = %d, want 4", rf.commitIndex)
	}
	// Heartbeat must not push commitIndex past what the leader's PrevLogIndex
	// proves we share, even if LeaderCommit is larger.
	r = ae(AppendEntriesArgs{Term: 3, PrevLogIndex: 3, PrevLogTerm: 3, LeaderCommit: 100})
	if !r.Success || rf.commitIndex != 4 {
		t.Fatalf("heartbeat commit wrong: commitIndex=%d %+v", rf.commitIndex, r)
	}
}

func TestCommitOnlyCurrentTermByCounting3B(t *testing.T) {
	rf := newTestRaft()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = 3
	rf.state = leader
	rf.log = []LogEntry{le(0, nil), le(1, "a"), le(2, "b")} // index 2 is from an older term
	rf.matchIndex = []int{2, 2, 0}                          // replicated on a majority

	rf.advanceCommitLocked()
	if rf.commitIndex != 0 {
		t.Fatalf("committed an old-term entry by counting replicas (Figure 8): commitIndex=%d", rf.commitIndex)
	}

	rf.log = append(rf.log, le(3, "c"))
	rf.matchIndex = []int{3, 3, 0}
	rf.advanceCommitLocked()
	if rf.commitIndex != 3 {
		t.Fatalf("current-term entry on majority should commit everything before it: commitIndex=%d", rf.commitIndex)
	}
}

func TestAppliedExactlyOnceInOrder3B(t *testing.T) {
	// Drive the applier directly with a slow consumer and repeated commit
	// notifications: entries must arrive once each, in order.
	ch := make(chan ApplyMsg)
	rf := newRaft(make([]*labrpc.Client, 3), 0, ch)
	go rf.applier()
	defer rf.Kill()
	rf.mu.Lock()
	for i := 1; i <= 20; i++ {
		rf.log = append(rf.log, le(1, i))
	}
	rf.commitIndex = 10
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	for i := 1; i <= 10; i++ {
		m := <-ch
		if m.Index != i || m.Command != i {
			t.Fatalf("got %+v, want index %d", m, i)
		}
		if i == 5 { // commit advances while the consumer is busy
			rf.mu.Lock()
			rf.commitIndex = 20
			rf.applyCond.Broadcast()
			rf.mu.Unlock()
		}
	}
	for i := 11; i <= 20; i++ {
		select {
		case m := <-ch:
			if m.Index != i {
				t.Fatalf("got index %d, want %d", m.Index, i)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for index %d", i)
		}
	}
	select {
	case m := <-ch:
		t.Fatalf("unexpected extra apply %+v", m)
	case <-time.After(100 * time.Millisecond):
	}
}

// Commands cross the (JSON) wire as gob bytes, so an int stays an int and a
// registered struct stays that struct on every peer.
func TestCommandCodecKeepsTypes3B(t *testing.T) {
	gob.Register(kvCommand{})
	for _, c := range []interface{}{7, "s", 3.5, true, kvCommand{"k", 9}, nil} {
		d, err := encodeCommand(c)
		if err != nil {
			t.Fatalf("encode %v: %v", c, err)
		}
		got, err := decodeCommand(d)
		if err != nil || got != c {
			t.Fatalf("round trip of %#v gave %#v (err %v)", c, got, err)
		}
	}
	type unregistered struct{ X int }
	if _, err := encodeCommand(unregistered{1}); err == nil {
		t.Fatalf("unregistered type should fail to encode")
	}
	cfg := makeConfig(t, 1, false)
	defer cfg.cleanup()
	cfg.checkOneLeader()
	if idx, _, ok := cfg.rafts[0].Start(unregistered{1}); ok || idx != -1 {
		t.Fatalf("Start accepted an unencodable command")
	}
}
