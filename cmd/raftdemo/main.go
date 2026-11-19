// Command raftdemo starts a 3-peer Raft cluster over real TCP (labrpc),
// submits commands, crashes the leader, and shows a new leader taking over.
package main

import (
	"fmt"
	"net"
	"time"

	"lab2/labrpc"
	"lab2/raft"
)

const n = 3 // no of peers

func freeAddrs(k int) []string {
	var lns []net.Listener
	var addrs []string
	for i := 0; i < k; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		lns = append(lns, ln)
		addrs = append(addrs, ln.Addr().String())
	}
	for _, ln := range lns {
		ln.Close()
	}
	return addrs
}

func main() {
	addrs := freeAddrs(n)
	rafts := make([]*raft.Raft, n)
	servers := make([]*labrpc.Server, n)
	chans := make([]chan raft.ApplyMsg, n)
	for i := 0; i < n; i++ {
		peers := make([]*labrpc.Client, n)
		for j := range peers {
			peers[j], _ = labrpc.NewClient(addrs[j])
		}
		chans[i] = make(chan raft.ApplyMsg, 100)
		rafts[i] = raft.Make(peers, i, chans[i])
		servers[i], _ = labrpc.NewServer(rafts[i])
		if err := servers[i].Start(addrs[i]); err != nil {
			panic(err)
		}
	}
	findLeader := func(skip int) int {
		for tries := 0; tries < 100; tries++ {
			for i := 0; i < n; i++ {
				if i == skip {
					continue
				}
				if _, ok := rafts[i].GetState(); ok {
					return i
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		panic("no leader")
	}
	drain := func(label string) {
		time.Sleep(600 * time.Millisecond)
		for i := 0; i < n; i++ {
			for len(chans[i]) > 0 {
				m := <-chans[i]
				fmt.Printf("  [%s] peer %d applied index=%d command=%v\n", label, i, m.Index, m.Command)
			}
		}
	}

	l := findLeader(-1)
	term, _ := rafts[l].GetState()
	fmt.Printf("leader elected: peer %d (term %d)\n", l, term)
	for _, c := range []interface{}{"set x=1", "set y=2"} {
		idx, t, _ := rafts[l].Start(c)
		fmt.Printf("Start(%q) -> index=%d term=%d\n", c, idx, t)
	}
	drain("replicated")

	fmt.Printf("crashing leader peer %d\n", l)
	rafts[l].Kill()
	servers[l].Stop()
	l2 := findLeader(l)
	term2, _ := rafts[l2].GetState()
	fmt.Printf("new leader: peer %d (term %d)\n", l2, term2)
	idx, _, _ := rafts[l2].Start("set z=3")
	fmt.Printf("Start(\"set z=3\") on new leader -> index=%d\n", idx)
	drain("after failover")

	for i := 0; i < n; i++ {
		rafts[i].Kill()
		servers[i].Stop()
	}
}
