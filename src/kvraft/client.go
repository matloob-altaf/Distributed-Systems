package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id              int64
	lastKnownLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		RequestID: nrand(),
		ClerkID:   ck.id,
	}
	reply := GetReply{}

	index := ck.lastKnownLeader
	for reply.Err != OK {
		ok := ck.servers[index%len(ck.servers)].Call("RaftKV.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			index++
		} else if reply.Err == ErrNoKey {
			return ""
		}
	}

	ck.lastKnownLeader = index % len(ck.servers)

	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		RequestID: nrand(),
		ClerkID:   ck.id,
	}
	reply := PutAppendReply{}

	index := ck.lastKnownLeader
	for reply.Err != OK {
		ok := ck.servers[index%len(ck.servers)].Call("RaftKV.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			index++
		}
	}
	ck.lastKnownLeader = index % len(ck.servers)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
