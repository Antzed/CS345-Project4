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
	"sync/atomic"
	"time"
)

// Timing constants
const (
	raftElectionTimeout = 500 * time.Millisecond
	HeartbeatInterval   = 100 * time.Millisecond
)

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

// LogEntry
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

	// Persistent state on all servers (Figure 2)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers (Figure 2)
	commitIndex int
	lastApplied int

	// Volatile state for the leader (Figure 2)
	nextIndex  []int
	matchIndex []int

	// Additional state
	state                int
	resetElectionTimerCh chan struct{}
	heartbeatInterval    time.Duration

	dead int32 // 0 ⇒ alive, 1 ⇒ rf.Kill() has been called
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply now includes ConflictTerm and ConflictIndex
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex) // NEW
	e.Encode(rf.lastApplied) // NEW
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// func (rf *Raft) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 {
// 		return
// 	}
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	var term int
// 	var voted int
// 	var entries []LogEntry
// 	if d.Decode(&term) != nil ||
// 		d.Decode(&voted) != nil ||
// 		d.Decode(&entries) != nil {
// 		return
// 	} else {
// 		rf.currentTerm = term
// 		rf.votedFor = voted
// 		rf.log = entries
// 	}
// }

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.commitIndex) != nil || // NEW
		d.Decode(&rf.lastApplied) != nil { // NEW
		// state was corrupt – start fresh
		return
	}
	// keep internal invariants
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied = rf.commitIndex
	}
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// Check up-to-date-ness of candidate's log
	var upToDate bool
	lastTerm := rf.logTermAt(rf.lastLogIndex())
	lastIndex := rf.lastLogIndex()
	if args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true

		// reset election timer
		select {
		case rf.resetElectionTimerCh <- struct{}{}:
		default:
		}
	} else {
		reply.VoteGranted = false
	}
}

// AppendEntries RPC handler with conflict-term / conflict-index logic.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictTerm = 0
		reply.ConflictIndex = len(rf.log)
		return
	}

	// 2. If term > currentTerm, update term & convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// reset election timer
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}

	// 3. Check if PrevLogIndex is out of bounds
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = 0
		reply.ConflictIndex = len(rf.log)
		return
	}

	// 4. Check if term at PrevLogIndex matches
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		// find first index of this conflictTerm in log
		firstIndex := 0
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != conflictTerm {
				firstIndex = i + 1
				break
			}
			if i == 0 {
				firstIndex = 0
			}
		}
		reply.Success = false
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = firstIndex
		return
	}

	if args.PrevLogIndex < rf.commitIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.commitIndex
		return
	}

	// 5. Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				// conflict: delete the existing entry and everything after
				rf.log = rf.log[:idx]
				if rf.lastApplied >= idx {
					rf.lastApplied = idx - 1
				}
				// append the new entries
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// follower's log is shorter: append remaining entries
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// 6. Update commitIndex if needed, then apply to state machine
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.persist()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// release lock while sending on applyCh
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
	}

	reply.Success = true
	reply.ConflictTerm = 0
	reply.ConflictIndex = 0
}

// sendRequestVote helper
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries helper
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start agreement on a new log entry. If not leader, return false.
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

	return index, term, true
}

// Kill (not used here).
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1) // mark dead
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Make creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

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
		heartbeatInterval:    HeartbeatInterval,
	}

	// initialize from persisted state
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
	for !rf.killed() {
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
			// send heartbeats
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

	// reset timer
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
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				muVotes.Lock()
				defer muVotes.Unlock()
				if reply.VoteGranted && termStarted == rf.currentTerm {
					rf.mu.Lock()
					votes++
					cond.Broadcast()
					rf.mu.Unlock()
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

	// wait for majority or term change
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
		if rf.currentTerm == termStarted && rf.state == Candidate {
			rf.state = Leader
			next := rf.lastLogIndex() + 1
			for i := range rf.peers {
				rf.nextIndex[i] = next
				rf.matchIndex[i] = 0
			}
			// send initial empty AppendEntries (heartbeats) to assert leadership
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
			// reset election timer immediately
			select {
			case rf.resetElectionTimerCh <- struct{}{}:
			default:
			}
		}
		rf.mu.Unlock()
	}
	muVotes.Unlock()
}

// leaderSendEntries now uses ConflictTerm/ConflictIndex on failure
func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {
	for !rf.killed() {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			// RPC failed (e.g., network), just return and retry from caller
			return
		}

		rf.mu.Lock()

		// If follower’s term is bigger, step down
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// If not still leader or terms don't match anymore, stop
		if rf.state != Leader || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// Advance matchIndex and nextIndex for this follower
			n := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[server] = n
			rf.nextIndex[server] = n + 1

			// Update commitIndex if a majority have appended the new entry
			for N := rf.commitIndex + 1; N <= rf.lastLogIndex(); N++ {
				cnt := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						cnt++
					}
				}
				if cnt*2 > len(rf.peers) && rf.log[N].Term == rf.currentTerm {
					rf.commitIndex = N
					rf.persist()
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
			// Conflict: use the returned term/index to jump nextIndex
			if reply.ConflictTerm != 0 {
				// If leader has any entry in ConflictTerm, jump to last index of that term + 1
				found := false
				for i := rf.lastLogIndex(); i >= 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						rf.nextIndex[server] = i + 1
						found = true
						break
					}
				}
				if !found {
					// Follower's term not in leader's log: jump to ConflictIndex
					rf.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				// No specific term, so follower’s log is shorter
				rf.nextIndex[server] = reply.ConflictIndex
			}

			// Rebuild new args based on updated nextIndex[server]
			nextIdx := rf.nextIndex[server]
			// If nextIdx <= 0, clamp to 1 (we always keep log[0] as a dummy)
			if nextIdx < 1 {
				nextIdx = 1
			}
			prev := nextIdx - 1
			args.Term = rf.currentTerm
			args.PrevLogIndex = prev
			args.PrevLogTerm = rf.log[prev].Term
			// Send the slice from prev+1 onward
			if prev+1 <= rf.lastLogIndex() {
				args.Entries = rf.log[prev+1:]
			} else {
				args.Entries = nil
			}
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			// Retry sending entries to this follower
			continue
		}
	}
}

// helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
