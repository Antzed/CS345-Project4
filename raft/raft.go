package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"bytes"
	"cs345/labgob"
	"cs345/labrpc"
	"math/rand"
	"sync"
	"time"
)

// Timing constants
const (
	raftElectionTimeout = 300 * time.Millisecond
	HeartbeatInterval   = 100 * time.Millisecond
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Logentry
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft server states
const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state for the leader
	nextIndex  []int
	matchIndex []int

	state                int
	resetElectionTimerCh chan struct{}
	heartbeatInterval    time.Duration
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// var term int
	// var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var entries []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&entries) != nil {
		return
	} else {
		rf.currentTerm = term
		rf.votedFor = voted
		rf.log = entries
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3, 4).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3).
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) logTermAt(index int) int {
	if index < 0 || index >= len(rf.log) {
		return 0
	}
	return rf.log[index].Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if term smaller than current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//if term bigger than current term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	//decide if grant vote
	upToDate := false
	lastLogTerm := rf.logTermAt(rf.lastLogIndex())
	lastLogindex := rf.lastLogIndex()

	if args.LastLogTerm < 0 || args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogindex) {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true

		//reset election timer
		select {
		case rf.resetElectionTimerCh <- struct{}{}:
		default:
		}

	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//refuse outdated request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//if request is more updated, self turn into follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	//reset election timer
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}

	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// append any new entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				// conflict: delete the existing entry and all that follow it
				rf.log = rf.log[:idx]
				if rf.lastApplied >= idx {
					rf.lastApplied = idx - 1
				}
				// append new entries
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// follower’s log is shorter: append remaining
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	//update local index base on leader
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
	}
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	term := rf.currentTerm

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIdx := rf.nextIndex[i]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIdx - 1,
			PrevLogTerm:  rf.log[nextIdx-1].Term,
			Entries:      rf.log[nextIdx:],
			LeaderCommit: rf.commitIndex,
		}
		go rf.leaderSendEntries(i, args)
	}
	// Your code here (4).

	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (3, 4).
	rf := &Raft{
		peers:                peers,
		persister:            persister,
		me:                   me,
		applyCh:              applyCh,
		currentTerm:          0,
		votedFor:             -1,
		log:                  make([]LogEntry, 1),
		commitIndex:          0,
		lastApplied:          0,
		nextIndex:            make([]int, len(peers)),
		matchIndex:           make([]int, len(peers)),
		state:                Follower,
		resetElectionTimerCh: make(chan struct{}, 1),
		heartbeatInterval:    100 * time.Millisecond,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.commitIndex > rf.lastLogIndex() {
		rf.commitIndex = rf.lastLogIndex()
	}

	if rf.lastApplied > rf.lastLogIndex() {
		rf.lastApplied = rf.lastLogIndex()
	}

	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 1)
	}

	next := rf.lastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}

	go rf.ticker()

	return rf
}

func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state != Leader {
			dur := rf.randomElectionTimeout()
			select {
			case <-time.After(dur):
				go rf.startElection()
			case <-rf.resetElectionTimerCh:
			}
		} else {
			//send heart beat
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.lastLogIndex(),
					PrevLogTerm:  rf.lastLogTerm(),
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				go rf.leaderSendEntries(i, args)
			}
			time.Sleep(rf.heartbeatInterval)
		}
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	return raftElectionTimeout + time.Duration(rand.Int63n(int64(raftElectionTimeout)))
}

func (rf *Raft) startElection() {
	// impelment this
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	termStarted := rf.currentTerm
	votes := 1
	nPeers := len(rf.peers)
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	rf.mu.Unlock()

	//reset timer
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}

	var muVotes sync.Mutex
	cond := sync.NewCond(&muVotes)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(sever int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(sever, args, reply) {
				muVotes.Lock()
				defer muVotes.Unlock()
				// if request granted, great, if not, its a follower
				if reply.VoteGranted && termStarted == rf.currentTerm {
					votes++
					cond.Broadcast()
				} else if reply.Term > termStarted {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	//wait for majoirty or lapse
	muVotes.Lock()
	for votes*2 <= nPeers {
		rf.mu.Lock()
		if rf.currentTerm != termStarted || rf.state != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		cond.Wait()
	}

	if votes*2 > nPeers {
		rf.mu.Lock()
		if rf.currentTerm == termStarted || rf.state != Candidate {
			rf.state = Leader
			next := rf.lastLogIndex() + 1
			for i := range rf.peers {
				rf.nextIndex[i] = next
				rf.matchIndex[i] = 0
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.lastLogIndex(),
					PrevLogTerm:  rf.lastLogTerm(),
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				go rf.leaderSendEntries(i, args)
			}
		}
		//send emepty appending entries
		select {
		case rf.resetElectionTimerCh <- struct{}{}:
		default:
		}
		rf.mu.Unlock()
	}
	muVotes.Unlock()

}

func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {

	for {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.state != Leader || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			n := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[server] = n
			rf.nextIndex[server] = n + 1

			for N := rf.commitIndex + 1; N <= rf.lastLogIndex(); N++ {
				count := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				if count*2 > len(rf.peers) && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
					for rf.lastApplied < rf.commitIndex {
						rf.lastApplied++
						applyMsg := ApplyMsg{
							CommandValid: true,
							Command:      rf.log[rf.lastApplied].Command,
							CommandIndex: rf.lastApplied,
						}
						rf.mu.Unlock()
						rf.applyCh <- applyMsg
						rf.mu.Lock()
					}
				}
			}
			rf.mu.Unlock()
			return
		} else {
			rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
			prev := rf.nextIndex[server] - 1
			args.Term = rf.currentTerm
			args.PrevLogIndex = prev
			args.PrevLogTerm = rf.log[prev].Term
			args.Entries = rf.log[prev+1:]
			rf.mu.Unlock()
		}
	}

}

// func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {
// 	// impelment this
// 	reply := &AppendEntriesReply{}
// 	if rf.sendAppendEntries(server, args, reply) {
// 		rf.mu.Lock()
// 		defer rf.mu.Unlock()
// 		if reply.Term > rf.currentTerm {
// 			rf.currentTerm = reply.Term
// 			rf.state = Follower
// 			rf.votedFor = -1
// 			rf.persist()
// 			return
// 		}
// 		if rf.state != Leader || args.Term != rf.currentTerm {
// 			return
// 		}
// 		if reply.Success {
// 			n := args.PrevLogIndex + len(args.Entries)
// 			rf.matchIndex[server] = n
// 			rf.nextIndex[server] = n + 1

// 			for N := rf.commitIndex + 1; N <= rf.lastLogIndex(); N++ {
// 				count := 1
// 				for i := range rf.peers {
// 					if i != rf.me && rf.matchIndex[i] >= N {
// 						count++
// 					}
// 				}
// 				//if more than half updated to N, and term is current time, apply the log
// 				if count*2 > len(rf.peers) && rf.log[N].Term == rf.currentTerm {
// 					rf.commitIndex = N

// 					for rf.lastApplied < rf.commitIndex {
// 						rf.lastApplied++
// 						applyMsg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
// 						rf.mu.Unlock()
// 						rf.applyCh <- applyMsg
// 						rf.mu.Lock()
// 					}
// 				}
// 			}
// 		} else {
// 			rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
// 			prev := rf.nextIndex[server] - 1

// 			args.Term = rf.currentTerm
// 			args.PrevLogIndex = prev
// 			args.PrevLogTerm = rf.log[prev].Term
// 			args.Entries = rf.log[prev+1:]
// 			go rf.leaderSendEntries(server, args)

// 		}
// 	}
// }

// min helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
