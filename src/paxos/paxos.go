package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format+"\n", a...)
	}
	return
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int                    // index into peers[]
	instances  map[int]*PaxosInstance //active paxos instances
	dones      []int
}

type PaxosInstance struct {
	n_p       int
	n_a       int
	v_a       interface{}
	v_decided interface{}
}

const (
	OK     = "OK"
	REJECT = "REJECT"
)

type ReplyType string

type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	Reply ReplyType
	N     int
	Na    int
	Va    interface{}
	Done  int //piggyback the Done value
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	Reply ReplyType
	N     int
	Done  int //piggyback the Done value
}

type DecidedArgs struct {
	Seq int
	V   interface{}
}

type DecidedReply struct {
}

func (px *Paxos) generateN() int {
	return int(time.Now().UnixNano())*len(px.peers) + px.me
}

func (px *Paxos) getPaxosInstance(seq int) *PaxosInstance {
	pxi, exists := px.instances[seq]
	if !exists {
		pxi = &PaxosInstance{}
		pxi.n_p = 0
		pxi.n_a = 0
		pxi.v_a = nil
		pxi.v_decided = nil
		px.instances[seq] = pxi
	}
	return pxi
}

//prepare handler
func (px *Paxos) Prepare(args *PrepareArgs, res *PrepareReply) error {
	DPrintf("%d received prepare for seq %d", px.me, args.Seq)
	px.mu.Lock()
	defer px.mu.Unlock()
	pxi := px.getPaxosInstance(args.Seq)
	if args.N > pxi.n_p {
		pxi.n_p = args.N
		res.Reply = OK
		res.N = args.N
		res.Na = pxi.n_a
		res.Va = pxi.v_a
	} else {
		res.Reply = REJECT
	}
	res.Done = px.dones[px.me]
	return nil
}

//accept handler
func (px *Paxos) Accept(args *AcceptArgs, res *AcceptReply) error {
	DPrintf("%d received accept for seq %d", px.me, args.Seq)
	px.mu.Lock()
	defer px.mu.Unlock()
	pxi := px.getPaxosInstance(args.Seq)
	if args.N >= pxi.n_p {
		pxi.n_p = args.N
		pxi.n_a = args.N
		pxi.v_a = args.V
		res.Reply = OK
		res.N = args.N
	} else {
		res.Reply = REJECT
	}
	res.Done = px.dones[px.me]
	return nil
}

//decide handler
func (px *Paxos) Decided(args *DecidedArgs, res *DecidedReply) error {
	DPrintf("%d received decide for seq %d", px.me, args.Seq)
	px.mu.Lock()
	defer px.mu.Unlock()
	pxi := px.getPaxosInstance(args.Seq)
	pxi.v_decided = args.V
	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	DPrintf("start seq: %d", seq)
	done, _ := px.Status(seq)
	go func(decided bool) {
		for !decided {
			n_proposal := px.generateN()
			highest_n_a := 0
			v_prime := v
			prepare_oks := 0
			for idx, peer := range px.peers {
				args := &PrepareArgs{Seq: seq, N: n_proposal}
				var reply PrepareReply
				if idx == px.me {
					px.Prepare(args, &reply)
				} else {
					call(peer, "Paxos.Prepare", args, &reply)
				}
				if reply.Reply == OK {
					DPrintf("Processed a PrepareOK at %d with seq %d", idx, seq)
					prepare_oks++
					if reply.Na > highest_n_a {
						highest_n_a = reply.Na
						v_prime = reply.Va
					}
				}
				px.dones[idx] = reply.Done
			}
			if prepare_oks < len(px.peers)/2+1 {
				time.Sleep(time.Second)
				continue
			}

			accept_oks := 0
			for idx, peer := range px.peers {
				args := &AcceptArgs{Seq: seq, N: n_proposal, V: v_prime}
				var reply AcceptReply
				if idx == px.me {
					px.Accept(args, &reply)
				} else {
					call(peer, "Paxos.Accept", args, &reply)
				}
				if reply.Reply == OK {
					DPrintf("Processed a AcceptOK at %d with seq %d", idx, seq)
					accept_oks++
				}
				px.dones[idx] = reply.Done
			}
			if accept_oks < len(px.peers)/2+1 {
				time.Sleep(time.Second)
				continue
			}

			decided = true

			for idx, peer := range px.peers {
				args := &DecidedArgs{Seq: seq, V: v_prime}
				var reply DecidedReply
				if idx == px.me {
					px.Decided(args, &reply)
				} else {
					call(peer, "Paxos.Decided", args, &reply)
				}
			}
		}
	}(done)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq > px.dones[px.me] {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	max := -1
	for seq, _ := range px.instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.dones[px.me]
	for idx := range px.dones {
		if min > px.dones[idx] {
			min = px.dones[idx]
		}
	}
	//fmt.Printf(" free ********** %d\n", px.me)
	for seq, _ := range px.instances {
		if seq <= min {
			delete(px.instances, seq)
			//fmt.Printf(" delseq %d ********** %d\n", px.me, seq)
		} else {
			//fmt.Printf(" reserve  seq %d ********** %d\n", px.me, seq)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	if seq < px.Min() {
		return false, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	pxi := px.getPaxosInstance(seq)
	value := pxi.v_decided
	return value != nil, value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]*PaxosInstance)
	px.dones = make([]int, len(peers))
	for idx := range px.dones {
		px.dones[idx] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
