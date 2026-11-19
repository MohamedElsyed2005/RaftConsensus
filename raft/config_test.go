package raft

// Test harness. The provided labrpc is a plain TCP/JSON RMI with no network
// simulation, so every directed link i->j goes through a small TCP proxy that
// the tests can switch off (partition), make lossy/slow, and use to count RPCs.
// The harness also verifies that every peer applies the same commands in the
// same order, each index exactly once.

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lab2/labrpc"
)

// link is a switchable proxy in front of one server, used by one client.
type link struct {
	ln         net.Listener
	mu         sync.Mutex
	target     string
	enabled    bool
	unreliable bool
	conns      map[net.Conn]struct{}
	count      *int64
}

func newLink(count *int64) *link {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	l := &link{ln: ln, enabled: true, conns: map[net.Conn]struct{}{}, count: count}
	go l.acceptLoop()
	return l
}

func (l *link) addr() string { return l.ln.Addr().String() }
func (l *link) close()       { l.ln.Close(); l.setEnabled(false) }

func (l *link) setTarget(a string) { l.mu.Lock(); l.target = a; l.mu.Unlock() }

func (l *link) setUnreliable(b bool) { l.mu.Lock(); l.unreliable = b; l.mu.Unlock() }

// setEnabled(false) also severs connections that are in flight.
func (l *link) setEnabled(on bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enabled = on
	if !on {
		for c := range l.conns {
			c.Close()
		}
		l.conns = map[net.Conn]struct{}{}
	}
}

func (l *link) track(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.enabled {
		return false
	}
	l.conns[c] = struct{}{}
	return true
}

func (l *link) acceptLoop() {
	for {
		c, err := l.ln.Accept()
		if err != nil {
			return
		}
		atomic.AddInt64(l.count, 1)
		go l.serve(c)
	}
}

func (l *link) serve(c net.Conn) {
	l.mu.Lock()
	target, unreliable := l.target, l.unreliable
	l.mu.Unlock()
	if !l.track(c) || target == "" {
		c.Close()
		return
	}
	if unreliable {
		if rand.Intn(100) < 10 { // request lost
			c.Close()
			return
		}
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
	}
	s, err := net.Dial("tcp", target)
	if err != nil {
		c.Close()
		return
	}
	if !l.track(s) {
		c.Close()
		s.Close()
		return
	}
	done := make(chan struct{}, 2)
	go func() { io.Copy(s, c); done <- struct{}{} }()
	go func() {
		if unreliable && rand.Intn(100) < 10 { // reply lost
			time.Sleep(10 * time.Millisecond)
			done <- struct{}{}
			return
		}
		io.Copy(c, s)
		done <- struct{}{}
	}()
	<-done
	c.Close()
	s.Close()
	l.mu.Lock()
	delete(l.conns, c)
	delete(l.conns, s)
	l.mu.Unlock()
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	n         int
	rafts     []*Raft
	servers   []*labrpc.Server
	links     [][]*link // links[i][j]: i's connection to j
	rpcCount  int64
	applyErr  []string
	connected []bool
	logs      []map[int]interface{}
	lastApply []int
}

func makeConfig(t *testing.T, n int, unreliable bool) *config {
	cfg := &config{
		t: t, n: n,
		rafts:     make([]*Raft, n),
		servers:   make([]*labrpc.Server, n),
		links:     make([][]*link, n),
		applyErr:  make([]string, n),
		connected: make([]bool, n),
		logs:      make([]map[int]interface{}, n),
		lastApply: make([]int, n),
	}
	for i := 0; i < n; i++ {
		cfg.logs[i] = map[int]interface{}{}
		cfg.connected[i] = true
		cfg.links[i] = make([]*link, n)
		for j := 0; j < n; j++ {
			cfg.links[i][j] = newLink(&cfg.rpcCount)
			cfg.links[i][j].setUnreliable(unreliable)
		}
	}
	for i := 0; i < n; i++ {
		cfg.startServer(i)
	}
	return cfg
}

func (cfg *config) setReliable(yes bool) {
	for i := range cfg.links {
		for _, l := range cfg.links[i] {
			l.setUnreliable(!yes)
		}
	}
}

func (cfg *config) cleanup() {
	for _, rf := range cfg.rafts {
		if rf != nil {
			rf.Kill()
		}
	}
	for _, s := range cfg.servers {
		if s != nil {
			s.Stop()
		}
	}
	for i := range cfg.links {
		for _, l := range cfg.links[i] {
			l.close()
		}
	}
}

func (cfg *config) startServer(i int) {
	peers := make([]*labrpc.Client, cfg.n)
	for j := 0; j < cfg.n; j++ {
		c, err := labrpc.NewClient(cfg.links[i][j].addr())
		if err != nil {
			cfg.t.Fatal(err)
		}
		peers[j] = c
	}
	applyCh := make(chan ApplyMsg)
	rf := Make(peers, i, applyCh)
	srv, err := labrpc.NewServer(rf)
	if err != nil {
		cfg.t.Fatal(err)
	}
	if err := srv.Start("127.0.0.1:0"); err != nil {
		cfg.t.Fatal(err)
	}
	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.servers[i] = srv
	cfg.mu.Unlock()
	for k := 0; k < cfg.n; k++ { // every client's link to i now points at the real server
		cfg.links[k][i].setTarget(srv.Address())
	}

	go func() {
		for {
			select {
			case m := <-applyCh:
				cfg.mu.Lock()
				errMsg := ""
				if m.UseSnapshot {
					errMsg = fmt.Sprintf("server %v got unexpected snapshot", i)
				} else {
					if m.Index != cfg.lastApply[i]+1 {
						errMsg = fmt.Sprintf("server %v applied index %v after %v (out of order or duplicate)",
							i, m.Index, cfg.lastApply[i])
					}
					cfg.lastApply[i] = m.Index
					for j := 0; j < len(cfg.logs); j++ {
						if old, ok := cfg.logs[j][m.Index]; ok && old != m.Command {
							errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
								m.Index, i, m.Command, j, old)
						}
					}
					cfg.logs[i][m.Index] = m.Command
				}
				if errMsg != "" {
					cfg.applyErr[i] = errMsg
				}
				cfg.mu.Unlock()
			case <-rf.killCh:
				return
			}
		}
	}()
}

func (cfg *config) setServerLinks(i int, on bool) {
	for j := 0; j < cfg.n; j++ {
		if j == i {
			continue
		}
		if on && !cfg.connected[j] {
			continue
		}
		cfg.links[i][j].setEnabled(on)
		cfg.links[j][i].setEnabled(on)
	}
}

func (cfg *config) connect(i int) {
	cfg.mu.Lock()
	cfg.connected[i] = true
	cfg.mu.Unlock()
	cfg.setServerLinks(i, true)
}

func (cfg *config) disconnect(i int) {
	cfg.mu.Lock()
	cfg.connected[i] = false
	cfg.mu.Unlock()
	cfg.setServerLinks(i, false)
}

func (cfg *config) rpcTotal() int { return int(atomic.LoadInt64(&cfg.rpcCount)) }

func (cfg *config) checkApplyErrors() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for _, e := range cfg.applyErr {
		if e != "" {
			cfg.t.Fatal(e)
		}
	}
}

func (cfg *config) isConnected(i int) bool {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	return cfg.connected[i]
}

// checkOneLeader waits for a leader among connected servers and checks that
// no term ever has two leaders.
func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < cfg.n; i++ {
			if cfg.isConnected(i) {
				if term, isLeader := cfg.rafts[i].GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		lastTermWithLeader := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", term, len(ls))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}
		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.isConnected(i) {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.isConnected(i) {
			if _, isLeader := cfg.rafts[i].GetState(); isLeader {
				cfg.t.Fatalf("expected no leader among connected servers, but %v claims to be leader", i)
			}
		}
	}
}

func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	var cmd interface{}
	for i := 0; i < len(cfg.rafts); i++ {
		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		errMsg := cfg.applyErr[i]
		cfg.mu.Unlock()
		if errMsg != "" {
			cfg.t.Fatal(errMsg)
		}
		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v", index, cmd, cmd1)
			}
			count++
			cmd = cmd1
		}
	}
	return count, cmd
}

func (cfg *config) one(cmd interface{}, expected int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second {
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				if index1, _, ok := rf.Start(cmd); ok {
					index = index1
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expected && cmd1 == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

func (cfg *config) begin(name string) { cfg.t.Logf("%s ...", name) }
