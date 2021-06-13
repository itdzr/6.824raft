package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "../labgob"

const (
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

type NodeState uint8

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

func (s NodeState) String() string { //for print struct
	switch {
	case s == Follower:
		return "Follower"
	case s == Candidate:
		return "Candidate"
	case s == Leader:
		return "Leader"
	}
	return "Unknown"
}
func (rf *Raft) String() string {
	return fmt.Sprintf("[node(%d), state(%v), term(%d),nextInt(%v),commit(%d),log[%v]]",
		rf.me, rf.state, rf.currentTerm, rf.nextIndex, rf.commitIndex, rf.logs)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm    int
	votedFor       int
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
	state          NodeState
	logs           []LogEntry
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain
	applyCh     chan ApplyMsg
	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("%v fails to recover from persist", rf)
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
}

// should be called with a lock
func (rf *Raft) convertTo(s NodeState) {

	if s == rf.state {
		return
	}
	DPrintf("Term %d:server %d convert from %v to %v\n",
		rf.currentTerm, rf.me, rf.state, s)
	pres := rf.state
	rf.state = s
	switch s {
	case Follower:
		if pres == Leader {
			rf.heartbeatTimer.Stop()
		}
		resetTimer(rf.electionTimer, randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
		rf.votedFor = -1
	case Leader:
		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		resetTimer(rf.heartbeatTimer, HeartbeatInterval)
	case Candidate:
		rf.state = Candidate
		rf.startElection()
	}
}

func (rf *Raft) broadcastHeartbeat() {

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()

			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[server] - 1
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				DPrintf("%+v state %v got broadcastHeartbeat response from node %d, success=%v, Term=%d",
					rf, rf.state, server, reply.Success, reply.Term)
				if reply.Success {

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}

				}
				rf.mu.Unlock()
			}

		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("rf[%d] logs[%v]", rf.me, rf.logs)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()

			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[server] - 1

			clogs := make([]LogEntry, len(rf.logs[prevLogIndex+1:]))

			copy(clogs, rf.logs[prevLogIndex+1:])
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				LogEntries:   clogs,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				DPrintf("%+v state %v got appendEntries logs[%v] response from node %d, success=%v, Term=%d",
					rf, rf.state, args.LogEntries, server, reply.Success, reply.Term)
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					//DPrintf("sccccccccccc:%d",rf.commitIndex)
					for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= i {
								count += 1
							}
						}

						if count > len(rf.peers)/2 {
							// most of nodes agreed on rf.logs[i]
							//DPrintf("setcc %d:%d",args.LeaderCommit,i)
							rf.setCommitIndex(i)
							break
						}
					}

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					} else {
						//for !reply.Success {
						//DPrintf("11")
						rf.nextIndex[server] -= 1

						//	prevLogIndex := rf.nextIndex[server] - 1
						//	clogs := make([]LogEntry, len(rf.logs)-prevLogIndex)
						//	copy(clogs, rf.logs[prevLogIndex+1:])
						//	args := &AppendEntriesArgs{
						//		Term:         rf.currentTerm,
						//		LeaderId:     rf.me,
						//		PrevLogIndex: prevLogIndex,
						//		PrevLogTerm:  rf.logs[prevLogIndex].Term,
						//		LogEntries:   clogs,
						//		LeaderCommit: rf.commitIndex,
						//	}
						//	if !rf.sendAppendEntries(server, args, reply){
						//		DPrintf("rpc error")
						//	}

						//}
					}

					// if term found, override it to
					// the first entry after entries in ConflictTerm

				}
				rf.mu.Unlock()
			}

		}(i)
	}
}

func (rf *Raft) startElection() {
	var count int64
	rf.currentTerm += 1
	lastLogIndex := len(rf.logs) - 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt64(&count, 1)
			continue
		}
		go func(server int) {

			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				//must lock after rpc
				DPrintf("%+v state %v got RequestVote response from node %d, VoteGranted=%v, Term=%d",
					rf, rf.state, server, reply.VoteGranted, reply.Term)
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt64(&count, 1)
					if atomic.LoadInt64(&count) > int64(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}

				} else {

					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
					}
				}
				rf.mu.Unlock()
			}

		}(i)

	}

}
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex

	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied {
		go func(start_idx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = start_idx + idx
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //请求选票的候选人的 Id 2A
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号 2B
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("RequestVote raft %+v args %+v", rf, args)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		// do not return here.
	}

	lastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
		// Receiver is more up-to-date, does not grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm // not used, for better logging
	}

	// reset timer after grant vote**
	resetTimer(rf.electionTimer,
		randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	// Your code here (2A, 2B).

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term //update its own term
		rf.convertTo(Follower)
		// do not return here.
	}

	resetTimer(rf.electionTimer,
		randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	//DPrintf("follower logs[%v] leader logs[%v]",rf.logs,args.LogEntries)
	if len(rf.logs)-1 > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	if len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm //for debug
		reply.Success = false
		return
	}

	if len(args.LogEntries) != 0 {
		rf.logs = append(rf.logs, args.LogEntries...)
	}
	//DPrintf("rffft%v:%d:%d",rf.logs,rf.me, rf.commitIndex)
	reply.Success = true
	reply.Term = rf.currentTerm

	//DPrintf("dsafa,%d,%d",args.LeaderCommit,rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {

		LastLogIndex := len(rf.logs) - 1
		if args.LeaderCommit <= LastLogIndex {
			rf.setCommitIndex(args.LeaderCommit)

		} else {
			rf.setCommitIndex(LastLogIndex)
		}
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	//DPrintf("rf%d,is%v",rf.me,isLeader)
	if isLeader {
		rf.mu.Lock()
		rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
		index = len(rf.logs) - 1
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.broadcastAppendEntries()
		rf.mu.Unlock()

	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.state = Follower

	rf.applyCh = applyCh

	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go func(node *Raft) {
		for {
			select {
			case <-node.electionTimer.C:
				node.mu.Lock()
				node.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper)) //need to be reset because election may fail
				if node.state == Follower {
					node.convertTo(Candidate)
				} else {
					node.startElection() //Avoid electoral defeat
				}
				node.mu.Unlock()
			case <-node.heartbeatTimer.C:
				node.mu.Lock()
				if node.state == Leader {
					node.broadcastHeartbeat()
					node.heartbeatTimer.Reset(HeartbeatInterval)
				}
				node.mu.Unlock()
			}
		}
	}(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randTimeDuration(lower time.Duration, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() { //if timer had been stopped or expired,the current time will be sent on C
		select {
		case <-timer.C: //try to drain from the channel
		default: //avoid timer.C become nil if timer had expired
		}
	}
	timer.Reset(d)
}
