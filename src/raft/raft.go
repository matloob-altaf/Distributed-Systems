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
	"labrpc"
	"math/rand"
	"sync"
	"time"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	id       int
	state    string
	isKilled bool

	currentTerm int
	votedFor    int
	leaderID    int

	lastHeartBeat time.Time
	lastEntrySent time.Time

	// TODO: Add log storage state
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()

	term = rf.currentTerm
	isleader = (rf.state == "Leader")

	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here
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
	// Your code here
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil {
		return
	}
}

// RequestVoteArgs -
type RequestVoteArgs struct {
	Term        int
	CandidateID int
	// lastLogIndex int
	// lastLogTerm  int
	// TODO: Last log index/term
}

// RequestVoteReply -
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote -
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
	} else if rf.votedFor == -1 || args.CandidateID == rf.votedFor {
		// TODO: Ensure candidates log is at least as up-to-date as our log
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	reply.Term = rf.currentTerm
	DPrintf("For server %d , term %d, state %s, case vote requested, for %d on term: %d. VoteGranted? %v", rf.id, rf.currentTerm, rf.state, args.CandidateID, args.Term, reply.VoteGranted)

	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.mu.Lock()
		DPrintf("For server %d , term %d, state %s, case RequestVote failed", rf.id, rf.currentTerm, rf.state)
		rf.mu.Unlock()
	}
	voteChan <- server
}

// AppendEntriesArgs - RPC arguments
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

// AppendEntriesReply - RPC response
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries - RPC function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm { // Become follower
		reply.Success = true
		rf.state = "Follower"
		rf.votedFor = -1
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.lastHeartBeat = time.Now()
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.mu.Lock()
		DPrintf("For server %d , term %d, state %s, case AppendEntries failed", rf.id, rf.currentTerm, rf.state)
		rf.mu.Unlock()
	}
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
// Start -
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()

	rf.isKilled = true
	DPrintf("For server %d , term %d, state %s, case killed", rf.id, rf.currentTerm, rf.state)

	rf.mu.Unlock()
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		id:        me,
		state:     "Follower",
	}

	DPrintf("For server %d , term %d, state %s, case created", rf.id, rf.currentTerm, rf.state)

	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimer()

	return rf
}

// electionTimeout return time between 150-300 ms (recommended election timeout value in paper)
func electionTimeout() time.Duration {
	return time.Duration(rand.Intn(300-150)+150) * time.Millisecond
}

func (rf *Raft) electionTimer() {
	timeout := electionTimeout()
	currentTime := <-time.After(timeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Start election
	if rf.state != "Leader" && currentTime.Sub(rf.lastHeartBeat) >= timeout {
		DPrintf("For server %d , term %d, state %s, case electionTimer timed out, value = %fs", rf.id, rf.currentTerm, rf.state, timeout.Seconds())
		go rf.startElection()
	} else if !rf.isKilled {
		go rf.electionTimer()
	}
}

func (rf *Raft) startElection() {
	// Increment currentTerm and vote for self
	rf.mu.Lock()
	rf.state = "Candidate"
	rf.currentTerm++
	rf.votedFor = rf.id

	DPrintf("For server %d , term %d, state %s, case election started", rf.id, rf.currentTerm, rf.state)

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.id}
	resps := make([]RequestVoteReply, len(rf.peers))

	// Reset election timer in case of split vote
	go rf.electionTimer()

	// Request votes from peers
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, voteChan, &args, &resps[i])
		}
	}
	rf.mu.Unlock()

	votes := 1
	for i := 0; i < len(resps); i++ {
		respIndex := <-voteChan
		if resps[respIndex].VoteGranted {
			votes++
		}
		if votes > int(len(resps)/2) {
			break // We can stop counting votes once we have majority
		}
	}

	rf.mu.Lock()

	// Ensure that we're still a candidate and that another election did not start
	if rf.state == "Candidate" && args.Term == rf.currentTerm {
		DPrintf("For server %d , term %d, state %s,  case election results, got %d out of %d votes", rf.id, rf.currentTerm, rf.state, votes, len(rf.peers))
		// If majority vote, become leader
		if votes > len(rf.peers)/2 {
			rf.state = "Leader"
			rf.leaderID = rf.id
			go rf.heartbeatTimer()
		}
	} else {
		DPrintf("For server %d , term %d, state %s, case election interupted, for term %d", rf.id, rf.currentTerm, rf.state, args.Term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()

	// Heartbeat message
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.id}
	resps := make([]AppendEntriesReply, len(rf.peers))

	DPrintf("For server %d , term %d, state %s, case sending heartbeat", rf.id, rf.currentTerm, rf.state)

	// Attempt to send heartbeats to all peers
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &resps[i])
		}
	}
	rf.lastEntrySent = time.Now()

	rf.mu.Unlock()
}

func (rf *Raft) heartbeatTimer() {
	rf.sendHeartbeats() // Send heartbeats to all peers as we've just become leader
	for {
		timeout := 15 * time.Millisecond // recomended timeout 0.5-20 ms
		currentTime := <-time.After(timeout)

		rf.mu.Lock()
		shouldSendHeartbeats := rf.state == "Leader" && currentTime.Sub(rf.lastEntrySent) >= timeout
		isDecomissioned := rf.isKilled
		rf.mu.Unlock()

		// If we're leader and haven't had an entry for a while, then send liveness heartbeat
		if _, isLeader := rf.GetState(); !isLeader || isDecomissioned {
			break
		} else if shouldSendHeartbeats {
			rf.sendHeartbeats()
		}
	}
}
