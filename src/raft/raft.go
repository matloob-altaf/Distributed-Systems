package raft

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// ApplyMsg - each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

// Raft - A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex

	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state         string
	isKilled      bool
	lastHeartBeat time.Time

	// Election state
	currentTerm int
	votedFor    int
	leaderID    int

	// Log state
	log         []Log
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex      []int
	matchIndex     []int
	sendAppendChan []chan struct{}
}

// Log - to store lo
type Log struct {
	Index   int
	Term    int
	Command interface{}
}

// GetState return currentTerm and whether this server believes it is the leader.
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

func (rf *Raft) lastLogEntry() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	return 0, 0
}

// --- RequestVote RPC ---

// RequestVoteArgs - RPC arguments
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply - RPC response
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote - RPC function
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.lastLogEntry()
	logUpToDate := func() bool {
		if lastLogTerm == args.LastLogTerm {
			return lastLogIndex <= args.LastLogIndex
		}
		return lastLogTerm < args.LastLogTerm
	}()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm && logUpToDate {
		rf.state = "Follower"
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.VoteGranted = true
	} else if (rf.votedFor == -1 || args.CandidateID == rf.votedFor) && logUpToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	DPrintf("For server %d , term %d, state %s, case vote requested, candidate %d on term: %d. VoteGranted? %v", rf.me, rf.currentTerm, rf.state, args.CandidateID, args.Term, reply.VoteGranted)

}

func (rf *Raft) sendRequestVote(server int, voteChan chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if voteChan <- server; !ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("For server %d , term %d, state %s, case RequestVote failed", rf.me, rf.currentTerm, rf.state)
	}
}

// --- Persistence ---

// Persist save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// readPersist restore previously persisted state.
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

// Kill - the tester calls Kill() when a Raft instance won't be needed again.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.isKilled = true
	DPrintf("For server %d , term %d, state %s, case killed", rf.me, rf.currentTerm, rf.state)

}

// --- AppendEntries RPC ---

// AppendEntriesArgs - RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []Log
	LeaderCommit int
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
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term >= rf.currentTerm {
		rf.state = "Follower"
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.leaderID == args.LeaderID {
		rf.lastHeartBeat = time.Now()
	}

	// Try to find supplied previous log entry match in our log
	prevLogIndex := -1
	for i, v := range rf.log {
		if v.Index == args.PrevLogIndex && v.Term == args.PrevLogTerm {
			prevLogIndex = i
			break
		}
	}

	if prevLogIndex >= 0 || (args.PrevLogIndex == 0 && args.PrevLogTerm == 0) {

		// Remove any inconsistent logs and find the index of the last consistent entry from the leader
		entriesIndex := 0
		for i := prevLogIndex + 1; i < len(rf.log); i++ {
			entryConsistent := func() bool {
				localEntry, leadersEntry := rf.log[i], args.LogEntries[entriesIndex]
				return localEntry.Index == leadersEntry.Index && localEntry.Term == leadersEntry.Term
			}
			if entriesIndex >= len(args.LogEntries) || !entryConsistent() {
				// Additional entries must be inconsistent, so let's delete them from our local log
				rf.log = rf.log[:i]
				break
			} else {
				entriesIndex++
			}
		}

		// Append all entries that are not already in our log
		if entriesIndex < len(args.LogEntries) {
			rf.log = append(rf.log, args.LogEntries[entriesIndex:]...)
		}

		// Update the commit index
		if args.LeaderCommit > rf.commitIndex {
			latestLogIndex := rf.log[len(rf.log)-1].Index
			if args.LeaderCommit < latestLogIndex {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = latestLogIndex
			}
		}
		reply.Success = true
	} else {
		reply.Success = false
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// updateCommitIndex updates commit index if there's an N greater than commit index
func (rf *Raft) updateCommitIndex() {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if v := rf.log[i]; v.Term == rf.currentTerm && v.Index > rf.commitIndex {
			replicationCount := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= v.Index {
					if replicationCount++; replicationCount > len(rf.peers)/2 { // Check to see if majority of nodes have replicated this
						rf.commitIndex = v.Index // Set index of this entry as new commit index
						break
					}
				}
			}
		} else {
			break
		}
	}
}

//Start agreement on a new log entry
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = 1
	if len(rf.log) > 0 {
		index = len(rf.log) + 1
	}

	rf.log = append(rf.log, Log{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command})

	return index, term, isLeader
}

// Make create a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		state:       "Follower",
		commitIndex: 0,
		lastApplied: 0,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimer()
	go rf.applyProcess(applyCh)

	return rf
}

// electionTimeout return a random timout value between 400-500 milliseconds on each call
func electionTimeout() time.Duration {
	return (500 + time.Duration(rand.Intn(500-400))) * time.Millisecond
}

// electionTimer keeps track of the electionTimeout and starts the elction if no leader or no heartbeat
func (rf *Raft) electionTimer() {
	timeout := electionTimeout()
	currentTime := <-time.After(timeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isKilled {
		// Start election process if we're not a leader and the haven't recieved a heartbeat for `electionTimeout`
		if rf.state != "Leader" && currentTime.Sub(rf.lastHeartBeat) >= timeout {
			DPrintf("For server %d , term %d, state %s, case electionTimer timed out, value = %fs", rf.me, rf.currentTerm, rf.state, timeout.Seconds())
			go rf.startElection()
		}
		go rf.electionTimer()
	}
}

// startElection conducts the election - and promote to leader ion winning
func (rf *Raft) startElection() {
	// Increment currentTerm and vote for self
	rf.mu.Lock()
	rf.state = "Candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("For server %d , term %d, state %s, case election started", rf.me, rf.currentTerm, rf.state)

	// Request votes from peers
	lastLogIndex, lastLogTerm := rf.lastLogEntry()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	voteChan := make(chan int, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendRequestVote(i, voteChan, &args, &replies[i])
		}
	}
	rf.mu.Unlock()

	// Count votes
	votes := 1
	for i := 0; i < len(replies); i++ {
		reply := replies[<-voteChan]

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = "Follower"
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}

		if reply.VoteGranted {
			votes++
		}
		if votes > len(replies)/2 { // Has majority vote
			// Ensure that we're still a candidate and that another election did not interrupt
			if rf.state == "Candidate" && args.Term == rf.currentTerm {
				// promote to leader
				go func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					rf.state = "Leader"
					rf.leaderID = rf.me

					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					rf.sendAppendChan = make([]chan struct{}, len(rf.peers))

					for i := range rf.peers {
						if i != rf.me {
							rf.nextIndex[i] = len(rf.log) + 1 // Should be initialized to leader's last log index + 1
							rf.matchIndex[i] = 0              // Index of highest log entry known to be replicated on server
							rf.sendAppendChan[i] = make(chan struct{}, 1)

							// Start routines for each peer which will be used to monitor and send log entries
							go rf.startAppendingLogs(i, rf.sendAppendChan[i])
						}
					}
				}()
			} else {
			}
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// startAppendingLogs calles the sendAppendEntriesHelper to start sending the log entries
func (rf *Raft) startAppendingLogs(server int, sendAppendChan chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)

	// Initial heartbeat
	rf.sendAppendEntriesHelper(server, sendAppendChan)
	lastEntrySent := time.Now()

	for {
		rf.mu.Lock()
		if rf.state != "Leader" || rf.isKilled {
			ticker.Stop()
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		select {
		case <-sendAppendChan: // Signal that we should send a new append to this peer
			lastEntrySent = time.Now()
			rf.sendAppendEntriesHelper(server, sendAppendChan)
		case currentTime := <-ticker.C: // If traffic has been idle, we should send a heartbeat
			if currentTime.Sub(lastEntrySent) >= (100 * time.Millisecond) {
				lastEntrySent = time.Now()
				rf.sendAppendEntriesHelper(server, sendAppendChan)
			}
		}
	}
}

// sendAppendEntriesHelper a helper function to keep sending heartbeats when no logs, and log if there are any,
// and to update it self after sending
func (rf *Raft) sendAppendEntriesHelper(server int, sendAppendChan chan struct{}) {
	rf.mu.Lock()

	if rf.state != "Leader" || rf.isKilled {
		rf.mu.Unlock()
		return
	}

	var entries []Log = []Log{}
	var prevLogIndex, prevLogTerm int = 0, 0

	lastLogIndex, _ := rf.lastLogEntry()

	if lastLogIndex > 0 && lastLogIndex >= rf.nextIndex[server] {
		for i, v := range rf.log { // Need to send logs beginning from index `rf.nextIndex[server]`
			if v.Index == rf.nextIndex[server] {
				if i > 0 {
					lastEntry := rf.log[i-1]
					prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
				}
				entries = make([]Log, len(rf.log)-i)
				copy(entries, rf.log[i:])
				break
			}
		}
	} else { // We're just going to send a heartbeat
		if len(rf.log) > 0 {
			lastEntry := rf.log[len(rf.log)-1]
			prevLogIndex, prevLogTerm = lastEntry.Index, lastEntry.Term
		}
	}

	reply := AppendEntriesReply{}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LogEntries:   entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		DPrintf("For server %d , term %d, state %s, case AppendEntries failed", rf.me, rf.currentTerm, rf.state)
	} else if reply.Success {
		if len(entries) > 0 {
			DPrintf("For server %d , term %d, case appended entries to the log of %d", rf.me, rf.currentTerm, server)
			lastAppended := entries[len(entries)-1]
			rf.matchIndex[server] = lastAppended.Index
			rf.nextIndex[server] = lastAppended.Index + 1
			rf.updateCommitIndex()
		} else {
			DPrintf("For server %d , term %d, case successful heartbeat from %d", rf.me, rf.currentTerm, server)
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.state = "Follower"
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			DPrintf("For server %d, term %d, case switched to follower", rf.me, rf.currentTerm)
		} else {
			if rf.nextIndex[server] >= 0 { // Log deviation, we should go back an entry and see if we can correct it
				rf.nextIndex[server]--
			}
			sendAppendChan <- struct{}{} // Signals to leader-peer process that appends need to occur
		}
	}
}

// applyProcess sends updated log to tester
func (rf *Raft) applyProcess(applyChan chan ApplyMsg) {
	for {
		rf.mu.Lock()

		if rf.commitIndex >= 0 && rf.commitIndex > rf.lastApplied {
			entries := make([]Log, rf.commitIndex-rf.lastApplied)
			copy(entries, rf.log[rf.lastApplied:rf.commitIndex])
			rf.mu.Unlock()

			for _, v := range entries { // Hold no locks so that slow local applies don't deadlock the system
				applyChan <- ApplyMsg{
					Index:   v.Index,
					Command: v.Command,
				}
			}

			rf.mu.Lock()
			rf.lastApplied += len(entries)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			<-time.After(10 * time.Millisecond)
		}
	}
}
