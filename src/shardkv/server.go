package shardkv

import (
	"shardmaster"
	"labrpc"
	"raft"
	"sync"
	"encoding/gob"
	"log"
	"bytes"
	"time"
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
	Seq   int      // latest request
	Reply GetReply // latest reply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk     // talk to master
	persist       *raft.Persister        // store snapshot
	shutdownCh    chan struct{}          // shutdown gracefully
	db            map[string]string      // data store
	notifyChs     map[int]chan struct{}  // per log entry
	duplicate     map[int64]*LatestReply // duplication detection table
	snapshotIndex int                    // snapshot
	config        shardmaster.Config     // current configuration
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// not leader ?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: Get(%q).\n", kv.me, kv.me, args.Key)

	// not responsible for key?
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
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
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// not leader ?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader %d receive rpc: PutAppend(%q => (%q,%q), (%d-%d).\n", kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	// not responsible for key?
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
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

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdownCh)
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if is leader
func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply?
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// must be persisted, in case of crashing before generating another snapshot
					kv.generateSnapshot(msg.Index)
					kv.mu.Unlock()
					continue
				}

				// have client's request? must filter duplicate command
				if msg.Command != nil && msg.Index > kv.snapshotIndex {
					cmd := msg.Command.(Op)
					kv.mu.Lock()
					if dup, ok := kv.duplicate[cmd.ClientID]; !ok || dup.Seq < cmd.SeqNo {
						switch cmd.Op {
						case "Get":
							kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo,
								Reply: GetReply{Value: kv.db[cmd.Key],}}
						case "Put":
							kv.db[cmd.Key] = cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo,}
						case "Append":
							kv.db[cmd.Key] += cmd.Value
							kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo,}
						default:
							DPrintf("[%d]: server %d receive invalid cmd: %v\n", kv.me, kv.me, cmd)
							panic("invalid command operation")
						}
						if ok {
							DPrintf("[%d]: server %d apply index: %d, cmd: %v (client: %d, dup seq: %d < %d)\n",
								kv.me, kv.me, msg.Index, cmd, cmd.ClientID, dup.Seq, cmd.SeqNo)
						}
					}

					// snapshot detection: up through msg.Index
					if needSnapshot(kv) {
						// save snapshot and notify raft
						DPrintf("[%d]: server %d need generate snapshot @ %d (%d vs %d), client: %d.\n",
							kv.me, kv.me, msg.Index, kv.maxraftstate, kv.persist.RaftStateSize(), cmd.ClientID)
						kv.generateSnapshot(msg.Index)
						kv.rf.NewSnapShot(msg.Index)
					}

					// notify channel
					if notifyCh, ok := kv.notifyChs[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.notifyChs, msg.Index)
					}
					kv.mu.Unlock()
				}
			}
		}
	}
}

func needSnapshot(kv *ShardKV) bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.maxraftstate < kv.persist.RaftStateSize() {
		return true
	}
	// abs < 10% of max
	var abs = kv.maxraftstate - kv.persist.RaftStateSize()
	var threshold = kv.maxraftstate / 10
	if abs < threshold {
		return true
	}
	return false
}

// which index?
func (kv *ShardKV) generateSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.db = make(map[string]string)
	kv.duplicate = make(map[int64]*LatestReply)

	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)
}

// get latest config: query shard master per 100ms
func (kv *ShardKV) getLatestConfig() {
	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			config := kv.mck.Query(-1)
			kv.mu.Lock()
			kv.config = config
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persist = persister
	kv.shutdownCh = make(chan struct{})
	kv.db = make(map[string]string)
	kv.notifyChs = make(map[int]chan struct{})
	kv.duplicate = make(map[int64]*LatestReply)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = shardmaster.Config{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// long-running work
	go kv.applyDaemon()     // get log entry from raft layer
	go kv.getLatestConfig() // query shard master for configuration
	return kv
}
