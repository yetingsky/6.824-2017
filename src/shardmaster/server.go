package shardmaster

import (
	"raft"
	"labrpc"
	"sync"
	"encoding/gob"
	"log"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config               // indexed by config num
	notifyChs  map[int]chan struct{}  // per log entry
	shutdownCh chan struct{}          // shutdown channel
	duplicate  map[int64]*LatestReply // duplication detection table
}

// Query Reply is the least common multiple of all replies
type LatestReply struct {
	Seq int // latest request
}

type Op struct {
	// Your data here.
	ClientID int64  // for duplicate request detection
	SeqNo    int    // sequence No.
	Op       string // must be one of "Join", "Leave", "Move" and "Query"

	Servers map[int][]string // args of "Join"
	GIDs    []int            // args of "Leave"
	Shard   int              // args of "Move"
	GID     int              // args of "Move"
	Num     int              // args of "Query" desired config number
}

// common function
func (sm *ShardMaster) requestAgree(cmd *Op, fillReply func(success bool)) {
	// not leader ?
	if _, isLeader := sm.rf.GetState(); !isLeader {
		fillReply(false)
		return
	}

	sm.mu.Lock()
	// duplicate put/append request
	if dup, ok := sm.duplicate[cmd.ClientID]; ok {
		// filter duplicate
		if cmd.SeqNo <= dup.Seq {
			sm.mu.Unlock()
			fillReply(true)
			return
		}
	}
	// notify raft to agreement
	index, term, _ := sm.rf.Start(cmd)

	ch := make(chan struct{})
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	// assume will succeed
	fillReply(true)

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership?
		curTerm, isLeader := sm.rf.GetState()

		// what if still leader, but different term? just let client retry
		if !isLeader || term != curTerm {
			fillReply(false)
			return
		}
	case <-sm.shutdownCh:
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Join", Servers: args.Servers,}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Leave", GIDs: args.GIDs}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Move", Shard: args.Shard, GID: args.GID}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{ClientID: args.ClientID, SeqNo: args.SeqNo, Op: "Query", Num: args.Num}
	sm.requestAgree(&cmd, func(success bool) {
		if success {
			reply.WrongLeader = false
			reply.Err = OK

			// if success, copy current or past Config
			sm.mu.Lock()
			sm.copyConfig(args.Num, &reply.Config)
			sm.mu.Unlock()
		} else {
			reply.WrongLeader = true
			reply.Err = ""
		}
	})
}

// should only be called when holding the lock
func (sm *ShardMaster) copyConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

// should only be called when holding the lock
func (sm *ShardMaster) joinConfig(servers map[int][]string) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	// add or update new gid-servers
	for k, v := range servers {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

// should only be called when holding the lock
func (sm *ShardMaster) leaveConfig(gids []int) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++

	for _, k := range gids {
		delete(config.Groups, k)
	}

	// 2. re-balance
	sm.rebalance(&config)
	sm.configs = append(sm.configs, config)
}

type Status struct {
	group, count int
}

// should only be called when holding the lock
// target: minimum shard movement
func (sm *ShardMaster) rebalance(config *Config) {
	groups := len(config.Groups)
	average := len(config.Shards)

	if groups == 0 {
		return
	}
	average = len(config.Shards) / groups       // shards per replica group
	left := len(config.Shards) - groups*average // extra shards after average

	// average may be zero, when group is more than shard
	if average == 0 {
		return
	}

	// group -> shard mapping
	var reverse = make(map[int][]int)
	for s, g := range config.Shards {
		reverse[g] = append(reverse[g], s)
	}

	// info
	var extra []Status
	var lacking []Status

	cnt1, cnt2 := 0, 0
	// if have new group?
	for k, _ := range config.Groups {
		if _, ok := reverse[k]; !ok {
			lacking = append(lacking, Status{k, average})
			cnt1 += average
		}
	}
	// if some group is removed?
	for k, v := range reverse {
		if _, ok := config.Groups[k]; !ok {
			extra = append(extra, Status{k, len(v)})
			cnt2 += len(v)
		} else {
			if len(v) > average {
				if left == 0 {
					extra = append(extra, Status{k, len(v) - average})
					cnt2 += len(v) - average
				} else if len(v) == average+1 {
					left--
				} else if len(v) > average+1 {
					extra = append(extra, Status{k, len(v) - average - 1})
					cnt2 += len(v) - average - 1
					left--
				}
			} else if len(v) < average {
				lacking = append(lacking, Status{k, average - len(v)})
				cnt1 += average - len(v)
			}
		}
	}

	// compensation for lacking
	if diff := cnt2 - cnt1; diff > 0 {
		if len(lacking) < diff {
			cnt := diff - len(lacking)
			for k, _ := range config.Groups {
				if len(reverse[k]) == average {
					lacking = append(lacking, Status{k, 0})
					if cnt--; cnt == 0 {
						break
					}
				}
			}
		}
		for i, _ := range lacking {
			lacking[i].count++
			if diff--; diff == 0 {
				break
			}
		}
	}

	// modify reverse
	for len(lacking) != 0 && len(extra) != 0 {
		e, l := extra[0], lacking[0]
		src, dst := e.group, l.group
		if e.count > l.count {
			// move l.count to l
			balance(reverse, src, dst, l.count)
			lacking = lacking[1:]
			extra[0].count -= l.count
		} else if e.count < l.count {
			// move e.count to l
			balance(reverse, src, dst, e.count)
			extra = extra[1:]
			lacking[0].count -= e.count
		} else {
			balance(reverse, src, dst, e.count)
			lacking = lacking[1:]
			extra = extra[1:]
		}
	}

	if len(lacking) != 0 || len(extra) != 0 {
		DPrintf("extra - lacking: %v <-> %v, %v\n", extra, lacking, config)
		panic("re-balance function bug")
	}

	for k, v := range reverse {
		for _, s := range v {
			config.Shards[s] = k
		}
	}
}

// helper function
func balance(data map[int][]int, src, dst, cnt int) {
	s, d := data[src], data[dst]
	if cnt > len(s) {
		DPrintf("cnd > len(s): %d <-> %d\n", cnt, len(s))
		panic("Oops...")
	}
	e := s[:cnt]
	d = append(d, e...)
	s = s[cnt:]

	data[src] = s
	data[dst] = d
}

// should only be called when holding the lock
func (sm *ShardMaster) moveConfig(shard int, gid int) {
	// 1. construct a new configuration
	config := Config{}
	sm.copyConfig(-1, &config)
	config.Num++
	config.Shards[shard] = gid

	// 2. no need to re-balance
	sm.configs = append(sm.configs, config)
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value
// state machine then notify related client if it's leader
func (sm *ShardMaster) applyDaemon() {
	for {
		select {
		case <-sm.shutdownCh:
			return
		case msg, ok := <-sm.applyCh:
			// have client's request? must filter duplicate command
			if ok && msg.Command != nil {
				cmd := msg.Command.(*Op)
				sm.mu.Lock()
				if dup, ok := sm.duplicate[cmd.ClientID]; !ok || dup.Seq < cmd.SeqNo {
					// update to latest
					sm.duplicate[cmd.ClientID] = &LatestReply{cmd.SeqNo}
					switch cmd.Op {
					case "Join":
						sm.joinConfig(cmd.Servers)
					case "Leave":
						sm.leaveConfig(cmd.GIDs)
					case "Move":
						sm.moveConfig(cmd.Shard, cmd.GID)
					case "Query":
						// no need to modify config
					default:
						panic("invalid command operation")
					}
				}
				// notify channel
				if notifyCh, ok := sm.notifyChs[msg.Index]; ok && notifyCh != nil {
					close(notifyCh)
					delete(sm.notifyChs, msg.Index)
				}
				sm.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdownCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(&Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.shutdownCh = make(chan struct{})
	sm.notifyChs = make(map[int]chan struct{})
	sm.duplicate = make(map[int64]*LatestReply)

	go sm.applyDaemon()
	return sm
}
