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
	"math/rand"
	"sync"
	"labrpc"
	"time"
)

// import "bytes"
// import "encoding/gob"
var rng = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// filter out too close timeout
var timeout = make(map[time.Duration]bool)

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
	isLeader          bool
	resetTimer        chan struct{} // for reset election timer
	electionTimer     *time.Timer   // election timer
	electionTimeout   time.Duration // 400~800ms
	heartbeatInterval time.Duration // 100ms

	currentTerm  int          // Persisted before responding to RPCs
	votedFor     int          // Persisted before responding to RPCs
	logs         []LogEntry   // Persisted before responding to RPCs
	commitCond   *sync.Cond   // for commitIndex update
	newEntryCond []*sync.Cond // for new log entry
	commitIndex  int          // Volatile state on all servers
	lastApplied  int          // Volatile state on all servers
	nextIndex    []int        // Leader only, reinitialized after election
	matchIndex   []int        // Leader only, reinitialized after election

	applyCh  chan ApplyMsg // outgoing channel to service
	shutdown chan struct{}
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
	select {
	case <-rf.shutdown:
		DPrintf("[%d-%s]: peer %d is closing, reject RV rpc request.\n", rf.me, rf, rf.me)
		return
	default:
	}

	DPrintf("[%d-%s]: rpc RV, from peer: %d, term: %d\n", rf.me, rf, args.CandidateID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			// convert to follower
			rf.currentTerm = args.Term
			rf.isLeader = false
			rf.votedFor = -1
		}

		// if is null (follower or stale leader) or itself is a candidate (which means itself)
		// check whether candidate's log is at least as update
		if rf.votedFor == -1 || rf.votedFor == rf.me || rf.votedFor == args.CandidateID {
			lastLogIdx := len(rf.logs) - 1
			lastLogTerm := rf.logs[lastLogIdx].Term

			if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx) ||
				args.LastLogTerm > lastLogTerm {

				rf.resetTimer <- struct{}{}

				rf.isLeader = false
				rf.votedFor = args.CandidateID
				reply.VoteGranted = true

				//DPrintf("[%d-%s]: peer %d vote to peer %d (last log idx: %d->%d, term: %d->%d)\n",
				//	rf.me, rf, rf.me, args.CandidateID, args.LastLogIndex, lastLogIdx, args.LastLogTerm, lastLogTerm)
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

type AppendEntriesReply struct {
	CurrentTerm int  // currentTerm, for leader to update itself
	Success     bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// extra for heartbeat from follower
	//ConflictTerm int // term of the conflicting entry
	FirstIndex int // the first index it stores for ConflictTerm
}

// AppendEntries handler, including heartbeat, must backup quickly
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	select {
	case <-rf.shutdown:
		DPrintf("[%d-%s]: peer %d is closing, reject AE rpc request.\n", rf.me, rf, rf.me)
		return
	default:
	}

	DPrintf("[%d-%s]: rpc AE, from peer: %d, term: %d\n", rf.me, rf, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		//DPrintf("[%d-%s]: AE failed from leader %d. (heartbeat: leader's term < follower's term (%d < %d))\n",
		//	rf.me, rf, args.LeaderID, args.Term, rf.currentTerm)
		reply.CurrentTerm = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		if rf.isLeader {
			rf.isLeader = false
			rf.commitCond.Broadcast()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.newEntryCond[i].Broadcast()
				}
			}
		}
		rf.currentTerm = args.Term

		// give new leader a change
		rf.resetTimer <- struct{}{}
	}

	// compare commit index
	if rf.commitIndex <= args.LeaderCommit {
		// encounter a new leader or the old one
		if rf.isLeader {
			rf.isLeader = false
			rf.commitCond.Broadcast()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.newEntryCond[i].Broadcast()
				}
			}
		}
		rf.votedFor = args.LeaderID
	} else {
		// if leader or if not my leader, reject AE
		if rf.isLeader {
			reply.Success = false
			DPrintf("[%d-%s]: leader %d get heartbeat from stale leader %d\n",
				rf.me, rf, rf.me, args.LeaderID)
			return
		} else if rf.votedFor != args.LeaderID {
			reply.Success = false
			DPrintf("[%d-%s]: peer %d get heartbeat from stale leader %d\n",
				rf.me, rf, rf.me, args.LeaderID)
			return
		}
	}

	// valid AE, reset election timer
	rf.resetTimer <- struct{}{}

	preLogIdx, preLogTerm := 0, 0
	if len(rf.logs) > args.PrevLogIndex {
		preLogIdx = args.PrevLogIndex
		preLogTerm = rf.logs[preLogIdx].Term
	}

	// last log is match
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true

		// check extra entry, if doesn't have in logs, append to local logs
		// or nil for heartbeat
		var last = preLogIdx
		for i, log := range args.Entries {
			if preLogIdx+i+1 < len(rf.logs) {
				if rf.logs[preLogIdx+i+1].Term != log.Term {
					rf.logs[preLogIdx+i+1] = log
					last = preLogIdx + i + 1
				}
			} else {
				rf.logs = append(rf.logs, log)
				last = len(rf.logs) - 1
			}
		}
		//DPrintf("[%d-%s]: logs: %v\n", rf.me, rf, rf.logs)
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			if last < args.LeaderCommit {
				rf.commitIndex = last
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			// signal possible update commit index
			go func() { rf.commitCond.Broadcast() }()
		}
		if len(args.Entries) > 0 {
			//DPrintf("[%d-%s]: AE success from leader %d (%d cmd @ %d), commit index: l->%d, f->%d.\n",
			//	rf.me, rf, args.LeaderID, len(args.Entries), preLogIdx+1, args.LeaderCommit, rf.commitIndex)
		} else {
			//DPrintf("[%d-%s]: <heartbeat> current logs: %v\n", rf.me, rf, rf.logs)
		}
	} else {
		reply.Success = false

		// extra info for restore missing entries quickly: from original paper
		// "the term of the conflicting entry and the first index it stores from that term"
		var term = preLogTerm
		if args.PrevLogTerm < term {
			term = args.PrevLogTerm
		}
		i := 1
		for ; i < len(rf.logs); i++ {
			if rf.logs[i].Term >= term {
				break
			}
		}
		if i != len(rf.logs) {
			//reply.ConflictTerm = preLogTerm
			reply.FirstIndex = i
		} else {
			//reply.ConflictTerm = preLogTerm
			reply.FirstIndex = 1
		}
		DPrintf("[%d-%s]: leader %d should start @ log entry %d\n", rf.me, rf, args.LeaderID,
			reply.FirstIndex)

		if len(rf.logs) <= args.PrevLogIndex {
			DPrintf("[%d-%s]: AE failed from leader %d, leader has more logs (%d > %d).\n",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, len(rf.logs)-1)
		} else {
			DPrintf("[%d-%s]: AE failed from leader %d, pre idx/term mismatch (%d != %d, %d != %d).\n",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, preLogIdx, args.PrevLogTerm, preLogTerm)
		}
	}
}

// bool is not useful
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
	index, term, isLeader := -1, 0, false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	if rf.isLeader {
		log := LogEntry{rf.currentTerm, command}
		rf.logs = append(rf.logs, log)

		index = len(rf.logs) - 1
		term = rf.currentTerm
		isLeader = true

		//DPrintf("[%d-%s]: client add new entry (%d-%v), logs: %v\n", rf.me, rf, index, command, rf.logs)
		DPrintf("[%d-%s]: client add new entry (%d-%v)\n", rf.me, rf, index, command)

		// only update leader
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// has work to do
		for i := 0; i < len(rf.peers); i++ {
			rf.newEntryCond[i].Broadcast()
		}
	}
	return index, term, isLeader
}

// heartbeat with consistency checking
//func (rf *Raft) consistencyCheckDaemon(n int) {
//	var args AppendEntriesArgs
//	var reply AppendEntriesReply
//
//	rf.mu.Lock()
//	args.Term = rf.currentTerm
//	args.LeaderID = rf.me
//	args.LeaderCommit = rf.commitIndex
//	args.PrevLogIndex = rf.nextIndex[n] - 1
//	args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
//
//	if rf.nextIndex[n] == rf.matchIndex[n]+1 {
//		args.Entries = nil
//	} else if rf.nextIndex[n] < len(rf.logs) {
//		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[n]:]...)
//	} else {
//		args.Entries = nil
//	}
//	rf.mu.Unlock()
//
//	if rf.sendAppendEntries(n, &args, &reply) {
//		rf.mu.Lock()
//		defer rf.mu.Unlock()
//		if reply.Success {
//			// RPC and consistency check successful
//			if rf.nextIndex[n] < len(rf.logs) {
//				if len(args.Entries) > 0 {
//					rf.nextIndex[n] += len(args.Entries)
//				} else {
//					rf.nextIndex[n]++
//				}
//			}
//			if rf.nextIndex[n] > len(rf.logs) {
//				DPrintf("[%d-%s]: warning: peer %d's match index exceed log(%d-%d) <rpc success>\n",
//					rf.me, rf, n, rf.nextIndex[n], len(rf.logs))
//				rf.nextIndex[n] = len(rf.logs)
//			}
//			rf.matchIndex[n] = rf.nextIndex[n] - 1
//		} else {
//			// found a new leader, turn to follower
//			if reply.CurrentTerm > rf.currentTerm {
//				rf.currentTerm = reply.CurrentTerm
//				rf.isLeader = false
//				rf.votedFor = -1
//				return
//			}
//
//			// with extra info, leader can be more clever
//			if reply.FirstIndex <= 0 {
//				rf.nextIndex[n]--
//			} else {
//				rf.nextIndex[n] = reply.FirstIndex
//			}
//
//			// rf.nextIndex[n] may be 0, because of unstable net, when reach 0, reset to 1
//			if rf.nextIndex[n] <= 0 {
//				rf.nextIndex[n] = 1
//			} else if rf.nextIndex[n] > len(rf.logs) {
//				DPrintf("[%d-%s]: warning: peer %d's match index exceed log(%d-%d) <rpc fail>\n",
//					rf.me, rf, n, rf.nextIndex[n], len(rf.logs))
//				rf.nextIndex[n] = len(rf.logs)
//			}
//			return
//		}
//	}
//}

func (rf *Raft) consistencyCheckDaemon(n int) {
	for {
		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: leader %d close consistency check daemon.\n", rf.me, rf, rf.me)
			return
		default:
		}

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.newEntryCond[n].Wait()

		//DPrintf("[%d-%s]: workhorse wake up for peer %d\n", rf.me, rf, n)
		if rf.isLeader {
			var args AppendEntriesArgs
			var reply AppendEntriesReply

			args.Term = rf.currentTerm
			args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[n] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

			if rf.nextIndex[n] < len(rf.logs) {
				// new or missing entries
				args.Entries = append(args.Entries, rf.logs[rf.nextIndex[n]:]...)
				//DPrintf("[%d-%s]: AE to follower %d @ log entry %d\n", rf.me, rf, n, rf.nextIndex[n])
			} else {
				// heartbeat
				args.Entries = nil
				//DPrintf("[%d-%s]: <heartbeat> to follower %d @ log entry %d\n", rf.me, rf, n, rf.nextIndex[n]-1)
			}
			rf.mu.Unlock()

			// need to wait until timeout?, need a way to give up, timeout
			timeout := time.NewTimer(rf.heartbeatInterval)
			replyCh := make(chan struct{})
			go func() {
				rf.sendAppendEntries(n, &args, &reply)
				replyCh <- struct{}{}
			}()

			select {
			case <-timeout.C:
				//DPrintf("[%d-%s]: AE failed, no reply from remote server\n", rf.me, rf)
				// avoid goroutines leak
				go func() { <-replyCh }()
			case <-replyCh:
				if !timeout.Stop() {
					//DPrintf("[%d-%s]: timeout stop failed.", rf.me, rf)
					<-timeout.C
				}
				rf.mu.Lock()
				if reply.Success {
					// RPC and consistency check successful
					if len(args.Entries) > 0 {
						rf.nextIndex[n] += len(args.Entries)
					}
					if rf.nextIndex[n] > len(rf.logs) {
						//DPrintf("[%d-%s]: warning: peer %d's match index exceed log(%d-%d) <rpc success>\n",
						//	rf.me, rf, n, rf.nextIndex[n], len(rf.logs))
						rf.nextIndex[n] = len(rf.logs)
					}
					rf.matchIndex[n] = rf.nextIndex[n] - 1
				} else {
					//DPrintf("[%d-%s]: AE failed, term -> arg: %d, reply: %d\n", rf.me, rf, args.Term, reply.CurrentTerm)
					// found a new leader, turn to follower
					if reply.CurrentTerm > args.Term {
						// update if necessary
						if reply.CurrentTerm > rf.currentTerm {
							rf.currentTerm = reply.CurrentTerm
							rf.votedFor = -1
						}

						if rf.isLeader {
							rf.commitCond.Broadcast()
							for i := 0; i < len(rf.peers); i++ {
								if i != rf.me {
									rf.newEntryCond[i].Broadcast()
								}
							}
						}
						rf.isLeader = false
						rf.mu.Unlock()

						rf.resetTimer <- struct{}{}
						DPrintf("[%d-%s]: leader %d found new term, turn to follower.", rf.me, rf, rf.me)
						return
					}

					// with extra info, leader can be more clever
					if reply.FirstIndex <= 0 {
						rf.nextIndex[n]--
					} else {
						rf.nextIndex[n] = reply.FirstIndex
					}

					// rf.nextIndex[n] may be 0, because of unstable net, when reach 0, reset to 1
					if rf.nextIndex[n] <= 0 {
						rf.nextIndex[n] = 1
					} else if rf.nextIndex[n] > len(rf.logs) {
						DPrintf("[%d-%s]: warning: peer %d's match index exceed log(%d-%d) <rpc fail>\n",
							rf.me, rf, n, rf.nextIndex[n], len(rf.logs))
						rf.nextIndex[n] = len(rf.logs)
					}
				}
				rf.mu.Unlock()
			}
			// ignore false and retry indefinitely
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

// Check on this:
// There can only usefully be a single AppendEntries in flight from the
// leader to each follower: followers reject out-of-order
// AppendEntries, and the sender's nextIndex[] mechanism requires
// one-at-a-time. A provision for pipelining many AppendEntries would
// be better.

// startLogAgree issue appendEntries RPC, one log a time, should executed in order, one by one
// What if leader fail? Need a way to wake sleeping goroutine up and exit gracefully
func (rf *Raft) logEntryAgreeDaemon() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// work horse: append new entry and routines heartbeat
			go rf.consistencyCheckDaemon(i)
		}
	}
}

// heartbeatDaemon will exit when is not leader any more
// Only leader can issue heartbeat message.
func (rf *Raft) heartbeatDaemon() {
	for {
		time.Sleep(rf.heartbeatInterval)
		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: peer %d close heartbeatDaemon.\n", rf.me, rf, rf.me)
			return
		default:
		}

		if _, isLeader := rf.GetState(); isLeader {
			// reset leader's election timer
			rf.resetTimer <- struct{}{}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.newEntryCond[i].Broadcast() // have work to do
				}
			}
		} else {
			break
		}
	}
}

// updateCommitIndex find new commit id
func (rf *Raft) updateCommitIndex() {
	for {
		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: leader %d close update commit index daemon.\n", rf.me, rf, rf.me)
			return
		default:
		}
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			var N = len(rf.logs) - 1
			for ; N > rf.commitIndex; N-- {
				count := 0
				if rf.logs[N].Term == rf.currentTerm {
					for i := 0; i < len(rf.peers); i++ {
						if rf.matchIndex[i] >= N {
							count++
						}
					}
					// majority
					if count > len(rf.peers)/2 {
						//DPrintf("[%d-%s]: update commit index %d -> %d\n", rf.me, rf, rf.commitIndex, N)
						rf.commitIndex = N

						go func() { rf.commitCond.Broadcast() }()
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	close(rf.shutdown)
	// wait for 2s to shutdown gracefully
	time.Sleep(2 * time.Second)
}

// applyLogEntryDaemon exit when leader fail
func (rf *Raft) applyLogEntryDaemon() {
	for {
		var logs []LogEntry
		// wait
		rf.mu.Lock()
		rf.commitCond.Wait()
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.logs[last+1: cur+1])
		}
		rf.mu.Unlock()

		for i := 0; i < cur-last; i++ {
			// current command is replicated
			reply := ApplyMsg{
				Index:   last + i + 1,
				Command: logs[i].Command,
			}

			// reply to outer service
			// Note: must in the same goroutine, or may result in out of order apply
			rf.applyCh <- reply
		}
	}
}

// canvassVotes issues RequestVote RPC
func (rf *Raft) canvassVotes() {
	select {
	case <-rf.shutdown:
		DPrintf("[%d-%s]: peer %d shutdown canvass votes routine.\n", rf.me, rf, rf.me)
		return
	default:
	}

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
	go func() { wg.Wait(); close(replies) }()

	var votes = 1
	for reply := range replies {
		if reply.VoteGranted == true {
			if votes++; votes > peers/2 {
				rf.mu.Lock()
				rf.isLeader = true
				rf.mu.Unlock()

				DPrintf("[%d-%s]: peer %d become new leader\n", rf.me, rf, rf.me)

				// reset leader state
				rf.resetOnElection()

				// start work house for append new log entry from client
				go rf.logEntryAgreeDaemon()

				// new leader, start heartbeat daemon
				go rf.heartbeatDaemon()

				// it's leader's responsibility to update commitIndex
				go rf.updateCommitIndex()

				return
			} else {
				//DPrintf("[%d-%s]: peer %d's votes is not enough.(%d<%d)\n",
				//	rf.me, rf, rf.me, votes, peers/2)
			}
		} else if reply.CurrentTerm > voteArgs.Term {
			rf.mu.Lock()
			rf.isLeader = false
			rf.votedFor = -1 // return to follower
			rf.mu.Unlock()

			// reset timer
			rf.resetTimer <- struct{}{}
			return
		}
	}
}

func (rf *Raft) String() string {
	if rf.isLeader {
		return "l"
	}
	return "f"
}

func (rf *Raft) resetOnElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count := len(rf.peers)
	length := len(rf.logs)

	for i := 0; i < count; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = length
	}
}

// electionDaemon never exit, but can be reset
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: peer %d is shutdown.\n", rf.me, rf, rf.me)
			return
		case <-rf.resetTimer:
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			rf.electionTimer.Reset(rf.electionTimeout)

			rf.mu.Lock()
			DPrintf("[%d-%s]: peer %d election timeout, issue election @ term %d\n",
				rf.me, rf, rf.me, rf.currentTerm)
			rf.mu.Unlock()
			go rf.canvassVotes()
		}
	}
}

// generate election time: 20ms interval
func generateElectionTimeout(n int) time.Duration {
	var res = 400*time.Millisecond + time.Duration(n+20)*time.Millisecond
	for {
		var ok = true
		res = time.Millisecond * (400 + time.Duration(rng.Intn(100)*4))
		for j := -10; j < 10; j++ {
			if timeout[res+time.Duration(j)*time.Millisecond] {
				ok = false
				break
			}
		}
		if ok {
			timeout[res] = true
			return res
		}
	}
	return res
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.isLeader = false
	rf.votedFor = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// first index is 1
	rf.logs = make([]LogEntry, 1, 20)
	// placeholder
	rf.logs[0] = LogEntry{
		Term:    0,
		Command: nil,
	}

	// shutdown raft gracefully
	rf.shutdown = make(chan struct{})

	// matchIndex is initialized to leader last log index+1
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
	}

	// electionTimeout: 400~800 ms, 20 ms
	rf.electionTimeout = generateElectionTimeout(me)

	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.resetTimer = make(chan struct{})

	// commitCh
	rf.commitCond = sync.NewCond(&rf.mu)

	// new log entry
	rf.newEntryCond = make([]*sync.Cond, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.newEntryCond[i] = sync.NewCond(&rf.mu)
	}

	// heartbeat: 200ms
	rf.heartbeatInterval = time.Millisecond * 200

	DPrintf("peer %d : election(%s) heartbeat(%s)\n", rf.me, rf.electionTimeout, rf.heartbeatInterval)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// kick off election
	go rf.electionDaemon()

	// start apply log
	go rf.applyLogEntryDaemon()

	return rf
}
