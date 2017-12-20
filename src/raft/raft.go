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
	"bytes"
	"encoding/gob"
	"sort"
)

var rng = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// filter out too close timeout
var mu sync.Mutex
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

	CurrentTerm  int          // Persisted before responding to RPCs
	VotedFor     int          // Persisted before responding to RPCs
	Logs         []LogEntry   // Persisted before responding to RPCs
	commitCond   *sync.Cond   // for commitIndex update
	newEntryCond []*sync.Cond // for new log entry
	commitIndex  int          // Volatile state on all servers
	lastApplied  int          // Volatile state on all servers
	nextIndex    []int        // Leader only, reinitialized after election
	matchIndex   []int        // Leader only, reinitialized after election

	applyCh  chan ApplyMsg // outgoing channel to service
	shutdown chan struct{} // shutdown channel, shut raft instance gracefully
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
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
	rf.VotedFor = rf.me
	rf.CurrentTerm += 1

	args.Term = rf.CurrentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.Logs) - 1
	args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
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
		DPrintf("[%d-%s]: peer %d is shutting down, reject RV rpc request.\n", rf.me, rf, rf.me)
		return
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIdx := len(rf.Logs) - 1
	lastLogTerm := rf.Logs[lastLogIdx].Term

	DPrintf("[%d-%s]: rpc RV, from peer: %d, arg term: %d, my term: %d (last log idx: %d->%d, term: %d->%d)\n",
		rf.me, rf, args.CandidateID, args.Term, rf.CurrentTerm, args.LastLogIndex, lastLogIdx, args.LastLogTerm, lastLogTerm)

	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm {
		reply.CurrentTerm = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			// convert to follower
			rf.CurrentTerm = args.Term
			rf.isLeader = false
			rf.VotedFor = -1
		}

		// if is null (follower) or itself is a candidate (or stale leader) with same term
		if rf.VotedFor == -1 { //|| (rf.VotedFor == rf.me && !sameTerm) { //|| rf.votedFor == args.CandidateID {
			// check whether candidate's log is at-least-as update
			if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx) ||
				args.LastLogTerm > lastLogTerm {

				rf.resetTimer <- struct{}{}

				rf.isLeader = false
				rf.VotedFor = args.CandidateID
				reply.VoteGranted = true

				DPrintf("[%d-%s]: peer %d vote to peer %d (last log idx: %d->%d, term: %d->%d)\n",
					rf.me, rf, rf.me, args.CandidateID, args.LastLogIndex, lastLogIdx, args.LastLogTerm, lastLogTerm)
			}
		}
	}
	rf.persist()
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

	// extra info for heartbeat from follower
	ConflictTerm int // term of the conflicting entry
	FirstIndex   int // the first index it stores for ConflictTerm
}

// AppendEntries handler, including heartbeat, must backup quickly
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	select {
	case <-rf.shutdown:
		DPrintf("[%d-%s]: peer %d is shutting down, reject AE rpc request.\n", rf.me, rf, rf.me)
		return
	default:
	}

	DPrintf("[%d-%s]: rpc AE, from peer: %d, term: %d\n", rf.me, rf, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		//DPrintf("[%d-%s]: AE failed from leader %d. (heartbeat: leader's term < follower's term (%d < %d))\n",
		//	rf.me, rf, args.LeaderID, args.Term, rf.currentTerm)
		reply.CurrentTerm = rf.CurrentTerm
		reply.Success = false
		return
	}

	// for stale leader
	if rf.isLeader {
		rf.isLeader = false
		rf.commitCond.Broadcast()
		rf.wakeupConsistencyCheck()
	}
	// for straggler (follower)
	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
	}
	// valid AE, reset election timer
	rf.resetTimer <- struct{}{}

	preLogIdx, preLogTerm := 0, 0
	if len(rf.Logs) > args.PrevLogIndex {
		preLogIdx = args.PrevLogIndex
		preLogTerm = rf.Logs[preLogIdx].Term
	}

	// last log is match
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true

		// check extra entry, if doesn't have in logs, append to local logs
		// or nil for heartbeat
		var last = preLogIdx
		for i, log := range args.Entries {
			if preLogIdx+i+1 < len(rf.Logs) {
				rf.Logs[preLogIdx+i+1] = log
				last = preLogIdx + i + 1
			} else {
				rf.Logs = append(rf.Logs, log)
				last = len(rf.Logs) - 1
			}
		}
		// truncate to known matched
		rf.Logs = rf.Logs[:last+1]

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
			DPrintf("[%d-%s]: AE success from leader %d (%d cmd @ %d), commit index: l->%d, f->%d.\n",
				rf.me, rf, args.LeaderID, len(args.Entries), preLogIdx+1, args.LeaderCommit, rf.commitIndex)
		} else {
			//DPrintf("[%d-%s]: <heartbeat> current logs: %v\n", rf.me, rf, rf.logs)
		}
	} else {
		reply.Success = false

		// extra info for restore missing entries quickly: from original paper and lecture note
		// if follower rejects, includes this in reply:
		//
		// the follower's term in the conflicting entry
		// the index of follower's first entry with that term
		//
		// if leader knows about the conflicting term:
		// 		move nextIndex[i] back to leader's last entry for the conflicting term
		// else:
		// 		move nextIndex[i] back to follower's first index
		var first = 1
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 {
			// which means leader has more logs or follower has no log at all
			first = len(rf.Logs)
			reply.ConflictTerm = rf.Logs[first-1].Term
		} else {
			for i := preLogIdx - 1; i > 0; i-- {
				if rf.Logs[i].Term != preLogTerm {
					first = i + 1
					break
				}
			}
		}
		reply.FirstIndex = first
		if len(rf.Logs) <= args.PrevLogIndex {
			DPrintf("[%d-%s]: AE failed from leader %d, leader has more logs (%d > %d), reply: %d - %d.\n",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, len(rf.Logs)-1, reply.ConflictTerm, reply.FirstIndex)
		} else {
			DPrintf("[%d-%s]: AE failed from leader %d, pre idx/term mismatch (%d != %d, %d != %d).\n",
				rf.me, rf, args.LeaderID, args.PrevLogIndex, preLogIdx, args.PrevLogTerm, preLogTerm)
		}
	}
	rf.persist()
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
	select {
	case <-rf.shutdown:
		return index, term, isLeader
	default:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	if rf.isLeader {
		log := LogEntry{rf.CurrentTerm, command}
		rf.Logs = append(rf.Logs, log)

		index = len(rf.Logs) - 1
		term = rf.CurrentTerm
		isLeader = true

		//DPrintf("[%d-%s]: client add new entry (%d-%v), logs: %v\n", rf.me, rf, index, command, rf.logs)
		DPrintf("[%d-%s]: client add new entry (%d-%v)\n", rf.me, rf, index, command)

		// only update leader
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// has work to do
		rf.wakeupConsistencyCheck()
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) wakeupConsistencyCheck() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.newEntryCond[i].Broadcast()
		}
	}
}

func (rf *Raft) consistencyCheckDaemon(n int) {
	for {
		rf.mu.Lock()
		rf.newEntryCond[n].Wait()
		select {
		case <-rf.shutdown:
			rf.mu.Unlock()
			rf.wakeupConsistencyCheck()
			DPrintf("[%d-%s]: leader %d is closing consistency check daemon for peer %d.\n", rf.me, rf, rf.me, n)
			return
		default:
		}

		if rf.isLeader {
			var args AppendEntriesArgs
			var reply AppendEntriesReply

			args.Term = rf.CurrentTerm
			args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[n] - 1
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term

			if rf.nextIndex[n] < len(rf.Logs) {
				// new or missing entries
				args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[n]:]...)
				//DPrintf("[%d-%s]: AE to follower %d @ log entry %d\n", rf.me, rf, n, rf.nextIndex[n])
			} else {
				// heartbeat
				args.Entries = nil
				//DPrintf("[%d-%s]: <heartbeat> to follower %d @ log entry %d\n", rf.me, rf, n, rf.nextIndex[n]-1)
			}
			rf.mu.Unlock()

			// Wait until timeout? Need a way to give up (timeout)
			replyCh := make(chan bool, 1)
			go func() {
				ok := rf.sendAppendEntries(n, &args, &reply)
				replyCh <- ok
			}()

			select {
			case <-time.After(rf.heartbeatInterval / 2): // rpc timeout
				DPrintf("[%d-%s]: AE rpc to peer %d timeout, no valid reply from remote server\n",
					rf.me, rf, n)
			case ok := <-replyCh:
				if !ok {
					DPrintf("[%d-%s]: AE rpc to peer %d failed, no valid reply from remote server\n",
						rf.me, rf, n)
					return
				}
				rf.mu.Lock()
				if reply.Success {
					// RPC and consistency check successful
					rf.nextIndex[n] += len(args.Entries)
					if rf.nextIndex[n] > len(rf.Logs) {
						rf.nextIndex[n] = len(rf.Logs)
					}
					rf.matchIndex[n] = rf.nextIndex[n] - 1

					// it's leader's responsibility to update commitIndex
					rf.updateCommitIndex()
				} else {
					// found a new leader, turn to follower
					if reply.CurrentTerm > args.Term {
						// update if necessary
						if reply.CurrentTerm > rf.CurrentTerm {
							rf.CurrentTerm = reply.CurrentTerm
							rf.VotedFor = -1
						}
						if rf.isLeader {
							rf.isLeader = false
							rf.commitCond.Broadcast()
							rf.wakeupConsistencyCheck()
						}
						rf.persist()
						rf.mu.Unlock()

						rf.resetTimer <- struct{}{}
						DPrintf("[%d-%s]: leader %d found new term (heartbeat resp from peer %d), turn to follower.",
							rf.me, rf, rf.me, n)
						return
					}

					// with extra info, leader can be more clever
					// Does leader know conflicting term?
					var know, lastIndex = false, 0
					if reply.ConflictTerm != 0 {
						for i := len(rf.Logs) - 1; i > 0; i-- {
							if rf.Logs[i].Term == reply.ConflictTerm {
								know = true
								lastIndex = i
								DPrintf("[%d-%s]: leader %d have entry %d is the last entry in term %d.",
									rf.me, rf, rf.me, i, reply.ConflictTerm)
								break
							}
						}
						if know {
							if lastIndex > reply.FirstIndex {
								lastIndex = reply.FirstIndex
							}
							rf.nextIndex[n] = lastIndex
						} else {
							rf.nextIndex[n] = reply.FirstIndex
						}
					} else {
						rf.nextIndex[n] = reply.FirstIndex
					}

					// rf.nextIndex[n] may be 0, because of unstable net, when reach 0, reset to 1
					if rf.nextIndex[n] <= 0 {
						rf.nextIndex[n] = 1
					} else if rf.nextIndex[n] > len(rf.Logs) {
						DPrintf("[%d-%s]: warning: peer %d's match index exceed log(%d-%d) <rpc fail>\n",
							rf.me, rf, n, rf.nextIndex[n], len(rf.Logs))
						rf.nextIndex[n] = len(rf.Logs)
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
		if _, isLeader := rf.GetState(); isLeader {
			// reset leader's election timer
			rf.resetTimer <- struct{}{}
			rf.wakeupConsistencyCheck()
		} else {
			break
		}

		time.Sleep(rf.heartbeatInterval)

		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: peer %d is closing heartbeatDaemon.\n", rf.me, rf, rf.me)
			return
		default:
		}
	}
}

// updateCommitIndex find new commit id, must be called when hold lock
func (rf *Raft) updateCommitIndex() {
	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Ints(match)

	DPrintf("[%d-%s]: leader %d try to update commit index: %v @ term %d.\n",
		rf.me, rf, rf.me, rf.matchIndex, rf.CurrentTerm)

	target := match[len(rf.peers)/2]
	if rf.commitIndex < target {
		if rf.Logs[target].Term == rf.CurrentTerm {
			DPrintf("[%d-%s]: leader %d update commit index %d -> %d @ term %d\n",
				rf.me, rf, rf.me, rf.commitIndex, target, rf.CurrentTerm)
			rf.commitIndex = target
			go func() { rf.commitCond.Broadcast() }()
		} else {
			DPrintf("[%d-%s]: leader %d update commit index %d failed (log term %d != current Term %d)\n",
				rf.me, rf, rf.me, rf.commitIndex, rf.Logs[target].Term, rf.CurrentTerm)
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
	mu.Lock()
	delete(timeout, rf.electionTimeout)
	mu.Unlock()

	close(rf.shutdown)
	rf.commitCond.Broadcast()
	rf.wakeupConsistencyCheck()

	// wait 2 heartbeat interval to shutdown gracefully
	time.Sleep(2 * rf.heartbeatInterval)
}

// applyLogEntryDaemon exit when shutdown channel is closed
func (rf *Raft) applyLogEntryDaemon() {
	for {
		var logs []LogEntry
		// wait
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
			// check if need to shut down
			select {
			case <-rf.shutdown:
				rf.mu.Unlock()
				DPrintf("[%d-%s]: peer %d is shutting down apply log entry to client daemon.\n", rf.me, rf, rf.me)
				close(rf.applyCh)
				return
			default:
			}
		}
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.Logs[last+1: cur+1])
		}
		rf.mu.Unlock()

		for i := 0; i < cur-last; i++ {
			// current command is replicated, ignore nil command
			reply := ApplyMsg{
				Index:   last + i + 1,
				Command: logs[i].Command,
			}

			// reply to outer service
			DPrintf("[%d-%s]: peer %d apply %v to client.\n", rf.me, rf, rf.me, reply)
			// Note: must in the same goroutine, or may result in out of order apply
			rf.applyCh <- reply
		}
	}
}

// canvassVotes issues RequestVote RPC
func (rf *Raft) canvassVotes() {
	select {
	case <-rf.shutdown:
		DPrintf("[%d-%s]: peer %d is shutting down canvass votes routine.\n", rf.me, rf, rf.me)
		return
	default:
	}

	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)
	peers := len(rf.peers)

	// buffered channel, avoid goroutine leak
	replyCh := make(chan RequestVoteReply, peers)

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

				doneCh := make(chan bool, 1)

				// rpc timeout
				go func() {
					ok := rf.sendRequestVote(n, &voteArgs, &reply)
					doneCh <- ok
				}()

				select {
				case <-time.After(rf.electionTimeout / 2):
					DPrintf("[%d-%s]: canvassVotes() request rv rpc to peer %d timeout.\n", rf.me, rf, n)
				case ok := <-doneCh:
					if !ok {
						DPrintf("[%d-%s]: canvassVotes() request rv rpc to peer %d failed.\n", rf.me, rf, n)
						return
					}
					replyCh <- reply
				}
			}(i)
		}
	}
	// closer, in case
	go func() { wg.Wait(); close(replyCh) }()

	var votes = 1
	for reply := range replyCh {
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
				return
			}
		} else if reply.CurrentTerm > voteArgs.Term {
			rf.mu.Lock()
			rf.isLeader = false
			rf.VotedFor = -1 // return to follower
			rf.CurrentTerm = reply.CurrentTerm
			rf.persist()
			rf.mu.Unlock()

			// reset timer
			rf.resetTimer <- struct{}{}
			return
		}
	}

	// reset timer, wait another election interval
	// rf.resetTimer <- struct{}{}
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
	length := len(rf.Logs)

	for i := 0; i < count; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = length
		if i == rf.me {
			rf.matchIndex[i] = length - 1
		}
	}
}

// electionDaemon never exit, but can be reset
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdown:
			DPrintf("[%d-%s]: peer %d is shutting down.\n", rf.me, rf, rf.me)
			return
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("[%d-%s]: peer %d election timeout, issue election @ term %d\n",
				rf.me, rf, rf.me, rf.CurrentTerm)
			rf.mu.Unlock()
			go rf.canvassVotes()
			rf.electionTimer.Reset(rf.electionTimeout)
		}
	}
}

// generate election time: 20ms interval
func generateElectionTimeout(n int) time.Duration {
	mu.Lock()
	defer mu.Unlock()
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
	rf.VotedFor = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// first index is 1
	rf.Logs = make([]LogEntry, 1)
	// placeholder
	rf.Logs[0] = LogEntry{
		Term:    0,
		Command: nil,
	}

	// shutdown raft gracefully
	rf.shutdown = make(chan struct{})

	// matchIndex is initialized to leader last log index+1
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.Logs)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[%d-%s]: newborn election(%s) heartbeat(%s) term(%d) voted(%d)\n",
		rf.me, rf, rf.electionTimeout, rf.heartbeatInterval, rf.CurrentTerm, rf.VotedFor)

	// kick off election
	go rf.electionDaemon()

	// start apply log
	go rf.applyLogEntryDaemon()

	return rf
}
