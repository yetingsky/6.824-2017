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
	"sync"
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Log Entry
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	isLeader bool

	resetTimer      chan struct{}
	electionTimer   *time.Timer
	electionTimeout time.Duration // in millisecond, 500 ~ 800ms

	// heartbeat
	heartbeatInterval time.Duration // in millisecond, 150ms

	// Persistent state on all servers:
	// Updated on stable storage before responding to RPCs
	currentTerm int
	votedFor    int // -1 means follower when startup, leader will vote itself
	logs        []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	// Reinitialized after election
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// turn to candidate and vote to itself
	rf.votedFor = rf.me
	rf.currentTerm += 1

	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
	} else {
		// convert to follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		// if is null or candidateId (candidate itself)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// check whether candidate's log is at least as update
			lastLogIdx := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIdx].Term

			if (args.LastLogIndex == lastLogTerm && args.LastLogIndex >= lastLogIdx) ||
				args.LastLogTerm > lastLogTerm {

				rf.isLeader = false
				rf.votedFor = args.CandidateID
				reply.VoteGranted = true

				rf.resetTimer <- struct{}{}
			}
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

// Log Replication and HeartBeat
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

func (rf *Raft) fillAppendEntriesArgs(args *AppendEntriesArgs, heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = len(rf.logs) - 1
	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	args.LeaderCommit = rf.commitIndex
	if heartbeat {
		args.Entries = nil
	}
}

type AppendEntriesReply struct {
	CurrentTerm int  // currentTerm, for leader to update itself
	Success     bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if heartbeat message, suppress leader-election
	if len(args.Entries) == 0 {
		if args.Term >= rf.currentTerm {
			reply.Success = true

			// encounter a new leader, changed to follower
			if rf.isLeader {
				rf.isLeader = false
			}
			// reset election timer
			rf.resetTimer <- struct{}{}
		} else {
			reply.CurrentTerm = rf.currentTerm
			reply.Success = false
		}
	}
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
// may fail or lose an election.
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
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// process replay of appendEntries including heartbeat
func (rf *Raft) heartbeat(n int) {
	var args AppendEntriesArgs
	rf.fillAppendEntriesArgs(&args, true)

	var reply AppendEntriesReply
	if rf.sendAppendEntries(n, &args, &reply) {
		if !reply.Success {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// found a new leader, turn to follower
			if reply.CurrentTerm > rf.currentTerm {
				rf.currentTerm = reply.CurrentTerm
				rf.isLeader = false
				rf.votedFor = -1
			} else {
				// TODO: send log entry from leader
			}
		}
	}
}

// heartbeatDaemon will exit when peer is not leader any more
// Only leader can issue heartbeat message.
func (rf *Raft) heartbeatDaemon() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					// reset  itself's electionTimer
					rf.resetTimer <- struct{}{}
				} else {
					go rf.heartbeat(i)
				}
			}
			time.Sleep(rf.heartbeatInterval)
		} else {
			break
		}
	}
}

// canvassVotes issues RequestVote RPC
func (rf *Raft) canvassVotes() {
	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)
	peers := len(rf.peers)

	// buffered channel, avoid goroutine leak
	replies := make(chan RequestVoteReply, peers)

	var wg sync.WaitGroup
	for i := 0; i < peers; i++ {
		if i == rf.me {
			// reset itself's electionTimer
			rf.resetTimer <- struct{}{}
		} else {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				var reply RequestVoteReply
				if rf.sendRequestVote(n, &voteArgs, &reply) {
					replies <- reply
				}
			}(i)
		}
	}
	// closer, in case
	go func() {
		wg.Wait()
		close(replies)
	}()

	var votes = 1
	for reply := range replies {
		if reply.VoteGranted == true {
			if votes++; votes > peers/2 {
				rf.mu.Lock()
				rf.isLeader = true
				rf.mu.Unlock()

				// new leader, start heartbeat daemon
				go rf.heartbeatDaemon()
				return
			}
		} else if reply.CurrentTerm > voteArgs.Term {
			rf.mu.Lock()
			rf.isLeader = false
			rf.votedFor = -1 // return to follower
			rf.mu.Unlock()
			return
		}
	}
}

// electionDaemon never exit, but can be reset
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.resetTimer:
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			rf.electionTimer.Reset(rf.electionTimeout)
			go rf.canvassVotes()
		}
	}
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
	rf.isLeader = false
	rf.votedFor = -1 // leader will keep -1, a flag

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// start index from 1
	rf.logs = make([]LogEntry, 1)

	// matchIndex is initialized to leader last log index+1
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = len(rf.logs)
	}

	// electionTimeout: 400~800 ms
	rf.electionTimeout = time.Millisecond * (400 + time.Duration(rand.Int63()%400))
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.resetTimer = make(chan struct{})

	// heartbeat: 100ms
	rf.heartbeatInterval = time.Millisecond * 100

	DPrintf("peer %d : election(%s) heartbeat(%s)\n", rf.me, rf.electionTimeout, rf.heartbeatInterval)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// kick off election
	go rf.electionDaemon()

	return rf
}
