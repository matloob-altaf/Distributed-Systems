package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Command   string
	Key       string
	Value     string
	RequestID int64
	ClientID  int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isKilled bool

	requestHandler map[int]chan raft.ApplyMsg
	data           map[string]string
	latestRequests map[int64]int64 //ClientID -> last applied RequestID
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("case starting get")
	operation := Op{
		Command:   "Get",
		Key:       args.Key,
		ClientID:  args.ClerkID,
		RequestID: args.RequestID,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(operation)
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		DPrintf("Operation get, case wrong leader")
	} else {
		isRequestSuccessful := kv.isRequestSucceeded(index, operation)
		if !isRequestSuccessful {
			DPrintf("Operation get, case wrong leader")
			reply.WrongLeader = true
		} else {
			kv.mu.Lock()
			value, isPresent := kv.data[args.Key]
			if isPresent {
				DPrintf("Operation get, case value recieved successfully")
				reply.Err = OK
				reply.Value = value
			} else {
				DPrintf("Operation get, case failed to recieve value")
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}

}

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
		operation.Command = "Put"
	} else {
		DPrintf("Operation PutAppend, case it's Append")
		operation.Command = "Append"
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(operation)
	kv.mu.Unlock()

	if !isLeader {
		DPrintf("Operation PutAppend, case wrong leader")
		reply.WrongLeader = true
	} else {
		isRequestSuccessful := kv.isRequestSucceeded(index, operation)
		if !isRequestSuccessful {
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
	kv.requestHandler = make(map[int]chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.latestRequests = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.startApplyProcess()
	return kv
}

func (kv *RaftKV) startApplyProcess() {
	DPrintf("case starting apply process")
	for !kv.isKilled {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()

			operation := msg.Command.(Op)

			if operation.Command != "Get" {
				requestID, isPresent := kv.latestRequests[operation.ClientID]
				if !isPresent && requestID != operation.RequestID {
					if operation.Command == "Put" {
						kv.data[operation.Key] = operation.Value
					} else if operation.Command == "Append" {
						kv.data[operation.Key] += operation.Value
					}
					kv.latestRequests[operation.ClientID] = operation.RequestID
				} else {
					DPrintf("Apply process, case duplicate write request")
				}

				c, isPresent := kv.requestHandler[msg.Index]
				if isPresent {
					c <- msg
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *RaftKV) isRequestSucceeded(index int, operation Op) bool {
	DPrintf("case starting isRequestSucceeded")
	kv.mu.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	kv.requestHandler[index] = awaitChan
	kv.mu.Unlock()

	for {
		select {
		case msg := <-awaitChan:
			kv.mu.Lock()
			delete(kv.requestHandler, index)
			kv.mu.Unlock()

			if index == msg.Index && operation == msg.Command {
				return true
			} else {
				return false
			}

		case <-time.After(10 * time.Millisecond):
			kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				delete(kv.requestHandler, index)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()

		}
	}

}
