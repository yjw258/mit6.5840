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
	//	"bytes"
	"math/rand"
	// "sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	FOLLOWER int32 = iota
	CANDIDATE
	LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers'
	currentTerm    int
	votedFor       int
	logs           []LogEntry
	role           int32
	lastCommTime time.Time
	applyCh        chan ApplyMsg
	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
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
	isleader = rf.role == LEADER
	return term, isleader
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	// reinitialize nextIndex and matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	go rf.sendHeartBeat()
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.Term = -1
		reply.VoteGranted = false
	}
	if reply.VoteGranted {
		rf.lastCommTime = time.Now()
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("Server %d received AppendEntries from server %d, term: %d, prevLogIndex: %d, prevLogTerm: %d, entries: %v, leaderCommit: %d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("args.Term(%d) < rf.currentTerm(%d), term outdated, return", args.Term, rf.currentTerm)
		return
	} else {
		rf.becomeFollower(args.Term)
		rf.lastCommTime = time.Now()
	}

	if args.PrevLogIndex > rf.getLastLogIndex() || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintf("Server %d received AppendEntries from server %d, logs don't match, return", rf.me, args.LeaderId)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	i := 0
	if args.Entries != nil {
		DPrintf("Before append, Server %d logs: %v", rf.me, rf.logs)
		for i < len(args.Entries) {
			if args.PrevLogIndex+i+1 > rf.getLastLogIndex() {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
			if rf.logs[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.logs = rf.logs[:args.PrevLogIndex+i+1]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
			i++
		}
		DPrintf("After append, Server %d logs: %v", rf.me, rf.logs)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// get the index of last new entry
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		temp := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		DPrintf("Server %d updates commitIndex from %d to %d",rf.me,  temp, rf.commitIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	// If command received from client: append entry to local log, respond after entry applied to state machine
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	DPrintf("Server %d received command from client, command: %v", rf.me, command)
	DPrintf("Server %d logs: %v", rf.me, rf.logs)
	index = rf.getLastLogIndex()
	term = rf.currentTerm

	// replicate log entries to followers

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	rf.lastCommTime = time.Now()
	currentTerm := rf.currentTerm
	leaderLd := rf.me
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.logs[prevLogIndex].Term
			rf.mu.Unlock()
			go func(server int, currentTerm int, leaderLd int, nextIndex int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     leaderLd,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      nil,
					LeaderCommit: leaderCommit,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					}
					rf.mu.Unlock()
					if reply.Success {
						rf.mu.Lock()
						// update nextIndex and matchIndex for follower
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1

						// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
						N := rf.commitIndex
						for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
							count := 1
							for j := 0; j < len(rf.peers); j++ {
								if j != rf.me && rf.matchIndex[j] >= i {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm {
								N = i
							}
						}
						if N > rf.commitIndex {
							DPrintf("Server(leader) %d updates commitIndex from %d to %d", rf.me, rf.commitIndex, N)
							rf.commitIndex = N
						}

						rf.mu.Unlock()
						return
					} else if reply.Term <= args.Term {
						rf.mu.Lock()
						rf.nextIndex[server] = args.PrevLogIndex
						rf.mu.Unlock()
					}

					// DPrintf("Server %d received reply from server %d, term: %d, success: %t", rf.me, server, reply.Term, reply.Success)
				}

			}(i, currentTerm, leaderLd, nextIndex, prevLogIndex, prevLogTerm, leaderCommit)
		}
	}
}

func (rf *Raft) elec() {
	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}
	DPrintf("Server %d started election", rf.me)
	voteCount := 1
	// Increment currentTerm
	rf.currentTerm++
	// Vote for self
	rf.votedFor = rf.me
	// reset lastCommTime
	rf.lastCommTime = time.Now()
	defer rf.mu.Unlock()
	// Send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, voteCount *int) {
				rf.mu.Lock()
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.getLastLogIndex(),
					LastLogTerm:  rf.getLastLogTerm(),
				}
				rf.mu.Unlock()
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					if reply.VoteGranted && rf.currentTerm == args.Term{
						*voteCount++
						DPrintf("Term %d: Server %d received vote from server %d, voteCount: %d", rf.currentTerm, rf.me, server, *voteCount)
						// If votes received from majority of servers: become leader
						if *voteCount == len(rf.peers)/2+1 && rf.role == CANDIDATE {
							DPrintf("Server %d became leader", rf.me)
							rf.becomeLeader()
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
						}
					}
					rf.mu.Unlock()
				}
			}(i, &voteCount)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := 500 + (rand.Int63() % 200)
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		switch rf.role {
		case FOLLOWER:
			if time.Since(rf.lastCommTime) > time.Duration(electionTimeout)*time.Millisecond {
				rf.role = CANDIDATE
				go rf.elec()
			}
		case CANDIDATE:
			// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
			if time.Since(rf.lastCommTime) > time.Duration(electionTimeout)*time.Millisecond {
				go rf.elec()
			}
		case LEADER:
			// Send heartbeats to all servers to maintain authority
			if time.Since(rf.lastCommTime) > 150*time.Millisecond {
				go rf.sendHeartBeat()
			}
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// apply log entries to state machine
func (rf *Raft) applyer() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			// apply log[rf.lastApplied] to state machine
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// send log entries to followers
func (rf *Raft) replicaLogEntriesToFollowers() {
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			leaderLd := rf.me
			commitIndex := rf.commitIndex
			rf.mu.Unlock()

			// send log entries to followers
			for i := 0; i < len(rf.peers); i++ {
				rf.mu.Lock()
				// lastLogIndex := rf.getLastLogIndex()
				if i != rf.me && rf.nextIndex[i] <= rf.getLastLogIndex() {
					go func(server int, currentTerm int, leaderLd int, commitIndex int) {
						rf.mu.Lock()
						args := &AppendEntriesArgs{
							Term:         currentTerm,
							LeaderId:     leaderLd,
							PrevLogIndex: rf.nextIndex[server] - 1,
							PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
							LeaderCommit: commitIndex,
						}
						args.Entries = make([]LogEntry, len(rf.logs[rf.nextIndex[server]:]))
						copy(args.Entries, rf.logs[rf.nextIndex[server]:])
						rf.mu.Unlock()
						reply := &AppendEntriesReply{}
						if rf.sendAppendEntries(server, args, reply) {
							rf.mu.Lock()
							if reply.Term > args.Term {
								rf.becomeFollower(reply.Term)
								rf.mu.Unlock()
								return
							}
							if rf.currentTerm == args.Term {
								if reply.Success {
									
									// update nextIndex and matchIndex for follower
									rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
									rf.nextIndex[server] = rf.matchIndex[server] + 1

									// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
									N := rf.commitIndex
									for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
										count := 1
										for j := 0; j < len(rf.peers); j++ {
											if j != rf.me && rf.matchIndex[j] >= i {
												count++
											}
										}
										if count > len(rf.peers)/2 && rf.logs[i].Term == rf.currentTerm {
											N = i
										}
									}
									if N > rf.commitIndex {
										DPrintf("Server(leader) %d updates commitIndex from %d to %d", rf.me, rf.commitIndex, N)
										rf.commitIndex = N
									}

									rf.mu.Unlock()
									return
								} else if reply.Term <= args.Term {
									rf.nextIndex[server] = args.PrevLogIndex
									rf.mu.Unlock()
								}
							}
						}
					}(i, currentTerm, leaderLd, commitIndex)
				}
				rf.mu.Unlock()

			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		role:           FOLLOWER,
		votedFor:       -1,
		lastCommTime: time.Now(),
		currentTerm:    0,
		commitIndex:    0,
		lastApplied:    0,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		applyCh:        applyCh,
	}
	rf.logs[0] = LogEntry{Term: 0, Command: nil}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.replicaLogEntriesToFollowers()
	// start commiter goroutine to commit log entries
	go rf.applyer()

	return rf
}
