package kvraft

import (
	"cs345/labgob"
	"cs345/labrpc"
	"cs345/raft"
	"log"
	"sync"
	"time"
)

type Op struct {
	Command   string // "get" | "put" | "append"
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type Result struct {
	Command     string
	OK          bool
	ClientId    int64
	RequestId   int64
	WrongLeader bool
	Err         Err
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data     map[string]string   // key-value data
	ack      map[int64]int64     // client's latest request id (for the duplication check)
	resultCh map[int]chan Result // log index to result of applying that entry
}

// try to append the entry to raft servers' log and return result.
// result is valid if raft servers apply this entry before timeout.
func (kv *KVServer) appendEntryToLog(entry Op) Result {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return Result{OK: false}
	}

	kv.mu.Lock()
	if _, ok := kv.resultCh[index]; !ok {
		kv.resultCh[index] = make(chan Result, 1)
	}
	kv.mu.Unlock()

	select {
	case result := <-kv.resultCh[index]:
		if isMatch(entry, result) {
			return result
		}
		return Result{OK: false}
	case <-time.After(240 * time.Millisecond):
		return Result{OK: false}
	}
}

// check if the result corresponds to the log entry.
func isMatch(entry Op, result Result) bool {
	return entry.ClientId == result.ClientId && entry.RequestId == result.RequestId
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	entry := Op{}
	entry.Command = "get"
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	entry := Op{}
	entry.Command = args.Command
	entry.ClientId = args.ClientId
	entry.RequestId = args.RequestId
	entry.Key = args.Key
	entry.Value = args.Value

	result := kv.appendEntryToLog(entry)
	if !result.OK {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = result.Err
}

// apply operation on database and return result.
func (kv *KVServer) applyOp(op Op) Result {
	result := Result{}
	result.Command = op.Command
	result.OK = true
	result.WrongLeader = false
	result.ClientId = op.ClientId
	result.RequestId = op.RequestId

	switch op.Command {
	case "put":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] = op.Value
		}
		result.Err = OK
	case "append":
		if !kv.isDuplicated(op) {
			kv.data[op.Key] += op.Value
		}
		result.Err = OK
	case "get":
		if value, ok := kv.data[op.Key]; ok {
			result.Err = OK
			result.Value = value
		} else {
			result.Err = ErrNoKey
		}
	}
	kv.ack[op.ClientId] = op.RequestId
	return result
}

// check if the request is duplicated with request id.
func (kv *KVServer) isDuplicated(op Op) bool {
	lastRequestId, ok := kv.ack[op.ClientId]
	if ok {
		return lastRequestId >= op.RequestId
	}
	return false
}

// the tester calls Kill() when a KVServer instance won't be needed again.
func (kv *KVServer) Kill() {
	kv.rf.Kill()
}

func (kv *KVServer) Run() {
	for {
		msg := <-kv.applyCh
		kv.mu.Lock()

		op := msg.Command.(Op)
		result := kv.applyOp(op)
		if ch, ok := kv.resultCh[msg.CommandIndex]; ok {
			select {
			case <-ch: // this means duplicates
				log.Printf("Error: The result already exists")
			default:
			}
		} else {
			kv.resultCh[msg.CommandIndex] = make(chan Result, 1)
		}
		kv.resultCh[msg.CommandIndex] <- result

		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Result{})

	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.ack = make(map[int64]int64)
	kv.resultCh = make(map[int]chan Result)

	go kv.Run()

	return kv
}
