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
	"bytes"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Other state
	role              int32
	lastCommTime      time.Time
	applyCh           chan ApplyMsg
	lastIncludedIndex int
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
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
	return len(rf.logs) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()-rf.lastIncludedIndex].Term
}

func (rf *Raft) becomeLeader() {
	rf.role = LEADER
	// reinitialize nextIndex and matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
	}
	go rf.heartBeat()
	Debug(dTerm, "S%d T:%d -> S%d became leader", rf.me, rf.currentTerm, rf.me)
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.persist()
	Debug(dTerm, "S%d T:%d -> S%d became follower", rf.me, rf.currentTerm, rf.me)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, nil)
	Debug(dPersist, "S%d T:%d -> S%d: persist", rf.me, rf.currentTerm, rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		// error
		Debug(dError, "S%d T:%d -> S%d: readPersist error", rf.me, rf.currentTerm, rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
	Debug(dPersist, "S%d T:%d -> S%d: readPersist: currentTerm: %d, votedFor: %d, len(logs): %d", rf.me, rf.currentTerm, rf.me, rf.currentTerm, votedFor, len(logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.getLastLogIndex() {
		return
	}
	rf.logs = rf.logs[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, snapshot)
	// rf.persist()
	Debug(dLog2, "S%d T:%d -> S%d len(logs) after snapshot: %d", rf.me, rf.currentTerm, rf.me, len(rf.logs))
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog, "S%d T:%d -> S%d received installSnapshot from S%d, args: [Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedTerm: %d, len(Data): %d, Done: %t]", rf.me, rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data), args.Done)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.lastCommTime = time.Now()
	Debug(dTimer, "S%d T:%d -> S%d reset lastCommTime", rf.me, rf.currentTerm, rf.me)
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	// discard log entries up to LastIncludedIndex
	rf.logs = rf.logs[args.LastIncludedIndex-rf.commitIndex:]
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()
	// apply snapshot to state machine
	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
}

// func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
// 	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
// 	return ok
// }

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
	Debug(dVote, "S%d T:%d -> S%d received vote request from S%d, args: [Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d]", rf.me, rf.currentTerm, rf.me, args.CandidateId, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	if reply.VoteGranted {
		rf.lastCommTime = time.Now()
		Debug(dTimer, "S%d T:%d -> S%d reset lastCommTime", rf.me, rf.currentTerm, rf.me)
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
	XTerm   int
	XIndex  int
	Xlen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog, "S%d T:%d -> S%d received appendEntries from S%d, args: [Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, len(Entries): %d, LeaderCommit: %d]", rf.me, rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit)
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.Xlen = 0

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	rf.lastCommTime = time.Now()
	Debug(dTimer, "S%d T:%d -> S%d reset lastCommTime", rf.me, rf.currentTerm, rf.me)

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.Xlen = rf.getLastLogIndex() + 1
		return
	} else if rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term

		// find the first index of the term
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.logs[i-rf.lastIncludedIndex].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}
		reply.Xlen = rf.getLastLogIndex() + 1
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	if args.Entries != nil {
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex+i+1 > rf.getLastLogIndex() {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			}
			if rf.logs[args.PrevLogIndex+i+1-rf.lastIncludedIndex].Term != args.Entries[i].Term {
				rf.logs = rf.logs[:args.PrevLogIndex+i+1-rf.lastIncludedIndex]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// get the index of last new entry
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		temp := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		Debug(dCommit, "S%d T:%d -> S%d updates commitIndex from %d to %d", rf.me, rf.currentTerm, rf.me, temp, rf.commitIndex)
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
	tmp := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	Debug(dLog, "S%d T:%d -> S%d receives a command from client, command: %v", rf.me, rf.currentTerm, rf.me, command)
	Debug(dLog2, "S%d T:%d -> S%d len(logs) from %d to %d", rf.me, rf.currentTerm, rf.me, tmp, len(rf.logs))

	index = rf.getLastLogIndex()
	term = rf.currentTerm

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

func (rf *Raft) elec() {
	rf.mu.Lock()
	if rf.role != CANDIDATE {
		rf.mu.Unlock()
		return
	}

	voteCount := 1
	// Increment currentTerm
	rf.currentTerm++
	// Vote for self
	rf.votedFor = rf.me

	Debug(dVote, "S%d T:%d -> S%d started election", rf.me, rf.currentTerm, rf.me)

	// persist
	rf.persist()

	// reset lastCommTime
	rf.lastCommTime = time.Now()
	Debug(dTimer, "S%d T:%d -> S%d reset lastCommTime", rf.me, rf.currentTerm, rf.me)

	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	defer rf.mu.Unlock()
	// Send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, voteCount *int, currentTerm int, candidateId int, lastLogIndex int, lastLogTerm int) {
				rf.mu.Lock()
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				rf.mu.Unlock()
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, args, reply) {
					rf.mu.Lock()
					Debug(dVote, "S%d T:%d -> S%d received vote request from S%d, reply: [Term: %d, VoteGranted: %t]", rf.me, rf.currentTerm, rf.me, server, reply.Term, reply.VoteGranted)
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					} else if reply.VoteGranted && rf.role == CANDIDATE && rf.currentTerm == args.Term {
						*voteCount++
						Debug(dVote, "S%d T:%d -> S%d received vote, voteCount: %d", rf.me, rf.currentTerm, rf.me, *voteCount)
						// If votes received from majority of servers: become leader
						if *voteCount == len(rf.peers)/2+1 && rf.role == CANDIDATE {
							rf.becomeLeader()
						}
					}
					rf.mu.Unlock()
				}
			}(i, &voteCount, currentTerm, candidateId, lastLogIndex, lastLogTerm)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := 500 + (rand.Int63() % 200)
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role == FOLLOWER && time.Since(rf.lastCommTime) > time.Duration(electionTimeout)*time.Millisecond {
			rf.role = CANDIDATE
			Debug(dTerm, "S%d T:%d -> S%d became candidate", rf.me, rf.currentTerm, rf.me)
			go rf.elec()
		} else if rf.role == CANDIDATE && time.Since(rf.lastCommTime) > time.Duration(electionTimeout)*time.Millisecond {
			go rf.elec()
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
			msg := ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command, CommandIndex: rf.lastApplied}
			// apply log[rf.lastApplied] to state machine
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			Debug(dInfo, "S%d T:%d -> S%d apply log[%d]: %v", rf.me, rf.currentTerm, rf.me, rf.lastApplied, rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command)
			Debug(dInfo, "S%d T:%d -> S%d applyer: lastApplied: %d, commitIndex: %d", rf.me, rf.currentTerm, rf.me, rf.lastApplied, rf.commitIndex)
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// send log entries to followers
func (rf *Raft) heartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.currentTerm
		leaderId := rf.me
		leaderCommit := rf.commitIndex
		Debug(dLeader, "S%d T:%d -> S%d: lastIncludedIndex: %d, len(logs): %d", rf.me, rf.currentTerm, rf.me, rf.lastIncludedIndex, len(rf.logs))
		rf.mu.Unlock()

		// send log entries to followers
		for i := 0; i < len(rf.peers); i++ {
			rf.mu.Lock()
			if i != rf.me && rf.role == LEADER {
				go func(server int, currentTerm int, leaderId int, leaderCommit int) {
					rf.mu.Lock()
					args := &AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     leaderId,
						PrevLogIndex: rf.nextIndex[server] - 1,
						PrevLogTerm:  rf.logs[rf.nextIndex[server]-1-rf.lastIncludedIndex].Term,
						LeaderCommit: leaderCommit,
					}
					if rf.nextIndex[server] <= rf.getLastLogIndex() {
						args.Entries = make([]LogEntry, len(rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]))
						copy(args.Entries, rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:])
					} else {
						args.Entries = nil
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(server, args, reply) {
						rf.mu.Lock()
						Debug(dLog, "S%d T:%d -> S%d received appendEntries reply from S%d, reply: [Term: %d, Success: %t, XTerm: %d, XIndex: %d, Xlen: %d]", rf.me, rf.currentTerm, rf.me, server, reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.Xlen)
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
						} else if rf.role == LEADER && rf.currentTerm == args.Term && reply.Term == args.Term {
							if reply.Success {
								// update nextIndex and matchIndex for follower
								rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
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
									if count > len(rf.peers)/2 && rf.logs[i-rf.lastIncludedIndex].Term == rf.currentTerm {
										N = i
									}
								}
								if N > rf.commitIndex {
									Debug(dCommit, "S%d T:%d -> S%d(leader) updates commitIndex from %d to %d", rf.me, rf.commitIndex, rf.me, rf.commitIndex, N)
									rf.commitIndex = N
								}
							} else if reply.Xlen > 0 {
								if reply.XTerm == -1 {
									// follower's log is too short
									rf.nextIndex[server] = reply.Xlen
								} else if rf.logs[reply.XIndex-rf.lastIncludedIndex].Term == reply.XTerm {
									// leader has XTerm in its log, find the last index of XTerm
									rf.nextIndex[server] = len(rf.logs) + rf.lastIncludedIndex
									for i := reply.XIndex + 1 + rf.lastIncludedIndex; i < len(rf.logs); i++ {
										if rf.logs[i-rf.lastIncludedIndex].Term != reply.XTerm {
											rf.nextIndex[server] = i
											break
										}
									}
								} else {
									// leader doesn't have XTerm in its log
									rf.nextIndex[server] = reply.XIndex
								}
							}
						}
						rf.mu.Unlock()
					}
				}(i, currentTerm, leaderId, leaderCommit)
			}
			rf.mu.Unlock()
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
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		role:              FOLLOWER,
		votedFor:          -1,
		lastCommTime:      time.Now(),
		currentTerm:       0,
		commitIndex:       0,
		lastApplied:       0,
		logs:              make([]LogEntry, 1),
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		applyCh:           applyCh,
		lastIncludedIndex: 0,
	}
	rf.logs[0] = LogEntry{Term: 0, Command: nil}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start commiter goroutine to commit log entries
	go rf.applyer()

	return rf
}
