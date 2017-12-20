package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64  // client id
	SeqNo    int    // request sequence number
}

type LatestReply struct {
	seq   int         // latest request
	reply interface{} // latest reply
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persist   *raft.Persister
	db        map[string]string
	notifyChs map[int]chan struct{}

	// shutdown chan
	shutdownCh chan struct{}

	// duplication detection table
	duplicate map[int64]*LatestReply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: Get(%q).\n", kv.me, kv.me, args.Key)

	kv.mu.Lock()
	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.reply.(*GetReply).Value
			return
		}
	}

	cmd := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		// what if still leader, but different term? let client retry
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		kv.mu.Lock()
		delete(kv.notifyChs, index)
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// not leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: PutAppend(%q => (%q,%q), (%d-%d).\n", kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	kv.mu.Lock()
	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	// new request
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()

		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}
	case <-kv.shutdownCh:
		return
	}
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if is leader
func (kv *RaftKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply?
				if msg.UseSnapshot {
					kv.readSnapshot(msg.Snapshot)
				}

				// have client's request?
				if msg.Command != nil {
					cmd := msg.Command.(Op)
					kv.mu.Lock()
					dup, ok := kv.duplicate[cmd.ClientID]
					if !ok || dup.seq < cmd.SeqNo {
						switch cmd.Op {
						case "Get":
							kv.duplicate[cmd.ClientID] = &LatestReply{seq: cmd.SeqNo,
								reply: &GetReply{Value: kv.db[cmd.Key],}}
						case "Put":
							kv.db[cmd.Key] = cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{seq: cmd.SeqNo, reply: nil}
						case "Append":
							kv.db[cmd.Key] += cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{seq: cmd.SeqNo, reply: nil}
						default:
							DPrintf("[%d]: server %d receive invalid cmd: %v\n", kv.me, kv.me, cmd)
						}

						// snapshot detection: up through msg.Index
						if needSnapshot(kv) {
							// save snapshot and notify raft
							DPrintf("[%d]: server %d need generate snapshot @ %d.\n", kv.me, kv.me, msg.Index)
							kv.generateSnapshot()
							kv.rf.NewSnapShot(msg.Index)
						}
					}
					// notify channel
					notifyCh := kv.notifyChs[msg.Index]
					kv.mu.Unlock()

					// notify client if it is or was leader
					_, isLeader := kv.rf.GetState()
					if isLeader && notifyCh != nil {
						notifyCh <- struct{}{}
					} else if notifyCh != nil {
						// lose leadership, notify blocked chan receiver
						close(notifyCh)
					}
				}
			}
		}
	}
}

// 100bytes
func needSnapshot(kv *RaftKV) bool {
	if kv.maxraftstate == -1 {
		return false
	}
	if kv.maxraftstate > kv.persist.RaftStateSize() &&
		kv.maxraftstate-kv.persist.RaftStateSize() < 100 {
		return true
	}
	return false
}

// which index?
func (kv *RaftKV) generateSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.duplicate)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&kv.db)
	d.Decode(&kv.duplicate)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdownCh)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// store key-value pairs
	kv.db = make(map[string]string)
	kv.notifyChs = make(map[int]chan struct{})
	kv.persist = persister

	// shutdown channel
	kv.shutdownCh = make(chan struct{})

	// duplication detection table: client->seq no.-> reply
	kv.duplicate = make(map[int64]*LatestReply)

	// read snapshot when start
	kv.readSnapshot(kv.persist.ReadSnapshot())

	go kv.applyDaemon()

	return kv
}
