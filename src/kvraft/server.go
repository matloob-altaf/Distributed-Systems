package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

// Debug wether to print anything or not
const Debug = 0

// DPrintf prints statements based on Debug
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type cmdType int

//
const (
	Put cmdType = iota
	Append
	Get
)

// Op operation structure
type Op struct {
	// Your definitions here.
	Command   cmdType
	Key       string
	Value     string
	RequestID int64
	ClientID  int64
}

// RaftKV structure
type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isKilled bool

	requestHandlers map[int]chan raft.ApplyMsg
	data            map[string]string
	latestRequests  map[int64]int64 //ClientID -> last applied RequestID
}

// Get gets the value from reft
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Command:   Get,
		Key:       args.Key,
		ClientID:  args.ClerkID,
		RequestID: args.RequestID,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
	} else {
		isRequestSuccessful := kv.isRequestSucceeded(index, op)
		if !isRequestSuccessful {
			reply.WrongLeader = true
			DPrintf("Get: Failed, node is no longer leader")
		} else {
			kv.mu.Lock()
			if val, isPresent := kv.data[args.Key]; isPresent {
				DPrintf("Get: Succeeded for key: %s", args.Key)
				reply.Err = OK
				reply.Value = val
			} else {
				DPrintf("Get: Failed, failed for key: %s", args.Key)
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}
}

// PutAppend put or append a value into raft
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("case starting PutAppend")
	operation := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClerkID,
		RequestID: args.RequestID,
	}
	if args.Op == "Put" {
		DPrintf("Operation PutAppend, case it's put")
		operation.Command = Put
	} else {
		DPrintf("Operation PutAppend, case it's Append")
		operation.Command = Append
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(operation)
	kv.mu.Unlock()

	if !isLeader {
		DPrintf("Operation PutAppend, case wrong leader")
		reply.WrongLeader = true
	} else {
		isRequestisRequestSuccessfulful := kv.isRequestSucceeded(index, operation)
		if !isRequestisRequestSuccessfulful {
			DPrintf("Operation PutAppend, case wrong leader")
			reply.WrongLeader = true
		} else {
			DPrintf("Operation PutAppend, case done")
			reply.Err = OK
		}
	}
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
	kv.isKilled = true
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

	// Your initialization code here.
	kv.requestHandlers = make(map[int]chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.latestRequests = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.startApplyProcess()
	return kv
}

func (kv *RaftKV) startApplyProcess() {
	DPrintf("Starting apply process")
	for !kv.isKilled {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()

			op := msg.Command.(Op)

			// A client will make only one call into a clerk at a time.
			if op.Command != Get {
				if requestID, isPresent := kv.latestRequests[op.ClientID]; !(isPresent && requestID == op.RequestID) {
					if op.Command == Put {
						kv.data[op.Key] = op.Value
					} else if op.Command == Append {
						kv.data[op.Key] += op.Value
					}
					kv.latestRequests[op.ClientID] = op.RequestID
				} else {
					DPrintf("Write request de-duplicated for key: %s. RId: %d, CId: %d", op.Key, op.RequestID, op.ClientID)
				}
			}

			if request, isPresent := kv.requestHandlers[msg.Index]; isPresent {
				request <- msg // TODO: Should probably send value if Get, likely false linearizability due to race conditions
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *RaftKV) isRequestSucceeded(index int, operation Op) bool {
	DPrintf("case starting isRequestSucceeded")
	kv.mu.Lock()
	successChan := make(chan raft.ApplyMsg, 1)
	kv.requestHandlers[index] = successChan
	kv.mu.Unlock()

	for {
		select {
		case msg := <-successChan:
			kv.mu.Lock()
			delete(kv.requestHandlers, index)
			kv.mu.Unlock()

			if index == msg.Index && operation == msg.Command {
				return true
			}
			// unexpected message
			return false

		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				delete(kv.requestHandlers, index)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()

		}
	}

}
