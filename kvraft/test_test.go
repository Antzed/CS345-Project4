package kvraft

import (
	"cs345/linearizability"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

// get/put/putappend that keep counts
func Get(cfg *config, ck *Clerk, key string) string {
	v := ck.Get(key)
	cfg.op()
	return v
}

func Put(cfg *config, ck *Clerk, key string, value string) {
	ck.Put(key, value)
	cfg.op()
}

func Append(cfg *config, ck *Clerk, key string, value string) {
	ck.Append(key, value)
	cfg.op()
}

func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals it is done
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn ncli clients and wait until they are all done
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft (i.e., log
// size) shouldn't exceed 2*maxraftstate.
func GenericTest(t *testing.T, part string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")"

	const nservers = 5
	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)

	ck := cfg.makeClient(cfg.All())

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := ""
			key := strconv.Itoa(cli)
			Put(cfg, myck, key, last)
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key)
			checkClntAppends(t, i, v, j)
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
	}

	cfg.end()
}

// similar to GenericTest, but with clients doing random operations (and using a
// linearizability checker)
func GenericTestLinearizability(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + ", linearizability checks (" + part + ")"

	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)

	begin := time.Now()
	var operations []linearizability.Operation
	var opMu sync.Mutex

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			for atomic.LoadInt32(&done_clients) == 0 {
				key := strconv.Itoa(rand.Int() % nclients)
				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
				var inp linearizability.KvInput
				var out linearizability.KvOutput
				start := int64(time.Since(begin))
				if (rand.Int() % 1000) < 500 {
					Append(cfg, myck, key, nv)
					inp = linearizability.KvInput{Op: 2, Key: key, Value: nv}
					j++
				} else if (rand.Int() % 1000) < 100 {
					Put(cfg, myck, key, nv)
					inp = linearizability.KvInput{Op: 1, Key: key, Value: nv}
					j++
				} else {
					v := Get(cfg, myck, key)
					inp = linearizability.KvInput{Op: 0, Key: key}
					out = linearizability.KvOutput{Value: v}
				}
				end := int64(time.Since(begin))
				op := linearizability.Operation{Input: inp, Call: start, Output: out, Return: end}
				opMu.Lock()
				operations = append(operations, op)
				opMu.Unlock()
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		// wait for clients.
		for i := 0; i < nclients; i++ {
			<-clnts[i]
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
	}

	cfg.end()

	// log.Printf("Checking linearizability of %d operations", len(operations))
	// start := time.Now()
	ok := linearizability.CheckOperationsTimeout(linearizability.KvModel(), operations, linearizabilityCheckTimeout)
	// dur := time.Since(start)
	// log.Printf("Linearizability check done in %s; result: %t", time.Since(start).String(), ok)
	if !ok {
		t.Fatal("history is not linearizable")
	}
}

func TestBasic4C(t *testing.T) {
	// Test: one client (4C) ...
	GenericTest(t, "4C", 1, false, false, false, -1)
}

func TestConcurrent4C(t *testing.T) {
	// Test: many clients (4C) ...
	GenericTest(t, "4C", 5, false, false, false, -1)
}

func TestUnreliable4C(t *testing.T) {
	// Test: unreliable net, many clients (4C) ...
	GenericTest(t, "4C", 5, true, false, false, -1)
}

func TestUnreliableOneKey4C(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: concurrent append to same key, unreliable (4C)")

	Put(cfg, ck, "k", "")

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := Get(cfg, ck, "k")
	checkConcurrentAppends(t, vx, counts)

	cfg.end()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition4C(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	Put(cfg, ck, "1", "13")

	cfg.begin("Test: progress in majority (4C)")

	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	Put(cfg, ckp1, "1", "14")
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	done0 := make(chan bool)
	done1 := make(chan bool)

	cfg.begin("Test: no progress in minority (4C)")
	go func() {
		Put(cfg, ckp2a, "1", "15")
		done0 <- true
	}()
	go func() {
		Get(cfg, ckp2b, "1") // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(cfg, t, ckp1, "1", "14")
	Put(cfg, ckp1, "1", "16")
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	cfg.begin("Test: completion after heal (4C)")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(cfg, t, ck, "1", "15")

	cfg.end()
}

func TestManyPartitionsOneClient4C(t *testing.T) {
	// Test: partitions, one client (4C) ...
	GenericTest(t, "4C", 1, false, false, true, -1)
}

func TestManyPartitionsManyClients4C(t *testing.T) {
	// Test: partitions, many clients (4C) ...
	GenericTest(t, "4C", 5, false, false, true, -1)
}

func TestPersistOneClient4C(t *testing.T) {
	// Test: restarts, one client (4C) ...
	GenericTest(t, "4C", 1, false, true, false, -1)
}

func TestPersistConcurrent4C(t *testing.T) {
	// Test: restarts, many clients (4C) ...
	GenericTest(t, "4C", 5, false, true, false, -1)
}

func TestPersistConcurrentUnreliable4C(t *testing.T) {
	// Test: unreliable net, restarts, many clients (4C) ...
	GenericTest(t, "4C", 5, true, true, false, -1)
}

func TestPersistPartition4C(t *testing.T) {
	// Test: restarts, partitions, many clients (4C) ...
	GenericTest(t, "4C", 5, false, true, true, -1)
}

func TestPersistPartitionUnreliable4C(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (4C) ...
	GenericTest(t, "4C", 5, true, true, true, -1)
}

func TestPersistPartitionUnreliableLinearizable4C(t *testing.T) {
	// Test: unreliable net, restarts, partitions, linearizability checks (4C) ...
	GenericTestLinearizability(t, "4C", 15, 7, true, true, true, -1)
}
