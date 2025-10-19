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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc" // todo
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {	//note
	CommandValid bool
	Command      interface{} // tag go
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const HbInterval = 100 * time.Millisecond // ms
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// persistence state
	currentTerm int
	voteFor int
	log []LogEntry
	lastLogIndex int

	// volatile state
	commitIndex int
	lastApplied int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state ServerState
	// exlusive to leader
	// nextIndex[]
	// matchIndex[]

	hbTimer time.Time
	//startTime time.Time
	electionTimeout  int64 // ms note go time. 
	// note 可优化为原子变量 uint32 ms数
	//hbTimer Duration //ms
}

type ServerState int
const (
	Candidate ServerState = iota
	Follower
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term =  rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) becomeCandidate() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.state, rf.voteFor = Candidate, rf.me
	rf.currentTerm++
	rf.resetElectionTimeout()
}

func (rf *Raft) promoteLeader(vote int) {
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	fmt.Printf("[node%d(%d)|%d] become leader with %d votes\n", rf.me, rf.currentTerm, time.Now().Nanosecond(), vote)
	
	go rf.DoHeartbeat()
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[node%d(%d)] become follower with term=%d\n", rf.me, rf.currentTerm, term)
	if term <= rf.currentTerm { // mark <=
		return
	}
	rf.becomeFollowerWithoutLock(term)
	rf.voteFor = -1
}

// all server 2
func (rf *Raft) updateTerm(term int) {
	if term <= rf.currentTerm { // mark <=
		return
	}
	fmt.Printf("[node%d(%d)] become follower with term=%d\n", rf.me, rf.currentTerm, term)
	rf.becomeFollowerWithoutLock(term)
	rf.voteFor = -1
}

func (rf *Raft) becomeFollowerWithoutLock(term int) {
	rf.state, rf.currentTerm = Follower, term
	rf.resetElectionTimeout()
}


func (rf *Raft) resetElectionTimeout() { //todo resetHbTimeout
	min, trange := HbInterval * 3, HbInterval * 3 / 2
	timeout := int64(min) + rand.Int63n(int64(trange))
	atomic.StoreInt64(&rf.electionTimeout, timeout)
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&rf.electionTimeout))
}

func (rf *Raft) isHbTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return time.Now().After(rf.hbTimer)
}

func (rf *Raft) ResetHbTimer() {
	rf.resetElectionTimeout()
	rf.hbTimer = time.Now().Add(time.Duration(rf.electionTimeout))
}

func (rf *Raft) isMajority(vote int) bool {
	return vote > len(rf.peers) / 2
}

func (rf *Raft) Vote(term int, leaderId int) {
	rf.becomeFollowerWithoutLock(term)
	rf.voteFor = leaderId
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters! // tag go
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote2(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// ok := rf.mu.TryLock()
	// if !ok {
		// fmt.Printf("[node%d] acquire lock failed in RequestVote handler\n", rf.me);
		// return
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[node%d(term%d)] received requestVote from node%d(term%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm	// todo 还是返回最新的?
	// mark 1.先检查term
	if args.Term > rf.currentTerm {		
		rf.Vote(args.Term, args.CandidateId)
		rf.ResetHbTimer()
		reply.VoteGranted = true
		return
	}
	
	// 2.检查日志
	if args.LastLogIndex < rf.commitIndex { 
		reply.VoteGranted = false
		return
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && args.LastLogIndex >= rf.lastLogIndex {
		rf.Vote(args.Term, args.CandidateId)	
		rf.ResetHbTimer()		
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[node%d(term%d)] received requestVote from node%d(term%d)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm	// todo 还是返回最新的?

	// cond1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// all servers 2
	rf.updateTerm(args.Term)

	// cond2
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && args.LastLogIndex >= rf.lastLogIndex {
		rf.Vote(args.Term, args.CandidateId)	
		rf.ResetHbTimer()		
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
}

func (rf *Raft) updateTermWithoutLock(term int) {
	rf.currentTerm = term
	rf.voteFor = -1
	rf.state = Follower
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("[node%d(term%d)] received appendEntries from node%d(term%d)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.ResetHbTimer()

	// todo 复制另外处理
	if args.Term > rf.currentTerm {	// mark 说明未参与当前任期的选举 
		rf.updateTerm(args.Term)
		reply.Success = true
		return
	}

	// fmt.Printf("[node%d(%d)] AppendEntriesArgs prevLogIndex=%d, prevTerm=%d, len=%d\n", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(rf.log))
	if rf.lastLogIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...) // tag go 右开
	rf.lastLogIndex = args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit < rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
	}
	reply.Success = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	// todo 日志复制


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1) // tag go
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // tag go
	return z == 1
}

func (rf *Raft) doElection() {
	for !rf.killed() {
		time.Sleep(rf.getElectionTimeout())
		rf.mu.Lock()
		state := rf.state

		switch state {
		case Leader:
			rf.mu.Unlock()
			continue // tag go to next loop
		case Follower:
			if !time.Now().After(rf.hbTimer){
				fmt.Printf("[node%d] 111\n", rf.me)
				rf.mu.Unlock()
				break				
			}
			fallthrough	
		case Candidate:
			fmt.Printf("[node%d | %d] hb timeout\n", rf.me, time.Now().Nanosecond())
			rf.becomeCandidate()
			lastLogIndex := len(rf.log) - 1
			args := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, rf.log[lastLogIndex].Term}
			rf.mu.Unlock()
			rf.startElection(args)
		}
		fmt.Printf("222\n")
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) DoHeartbeat() {	
	fmt.Println("start doHeartbeat")
	for !rf.killed() {
		if !rf.needHeartbeat() {
			return
		}
		
		rf.mu.Lock()
		lastLogIndex := len(rf.log) - 1
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, lastLogIndex, rf.log[lastLogIndex].Term, []LogEntry{}, rf.commitIndex}
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func (peer int, args *AppendEntriesArgs) {	// mark asycn
				var reply AppendEntriesReply
				fmt.Printf("node%d sendAppendEntries to node%d\n", rf.me, peer)
				ok := rf.sendAppendEntries(peer, args, &reply)

				//fmt.Printf("[node%d(%d)] get appendentries reply=%t,term=%d\n",rf.me, rf.currentTerm, reply.Success, reply.Term)
				if !ok {
					fmt.Printf("node%d sendAppendEntries to node%d failed\n", rf.me, peer)
					return
				}
				if reply.Success {
					return
				}
				rf.becomeFollower(reply.Term)
			}(peer, args)
		}

		time.Sleep(HbInterval)
	}
}

func (rf *Raft) needHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) startElection(args *RequestVoteArgs) {
	cnt := 1
	for peer := range rf.peers { // tag go
		if peer == rf.me {
			continue
		}
		go func (peer int, args *RequestVoteArgs) { // mark async
			var reply RequestVoteReply
			fmt.Printf("[node%d] sendRequestVote to node%d\n", rf.me, peer)
			ok := rf.sendRequestVote(peer, args, &reply)
			if (!ok) {
				fmt.Printf("[node%d] sendRequestVote to node%d failed\n", rf.me, peer)
				return 
			}
			fmt.Printf("[node%d] get RequestVoteReply from node%d:vote=%t,term=%d\n", rf.me, peer, reply.VoteGranted, reply.Term) // tag print bool
			if reply.VoteGranted {
				rf.mu.Lock()
				cnt++;	// note 有并发, rf锁控制
				if rf.isMajority(cnt) {
					rf.promoteLeader(cnt)
				}
				rf.mu.Unlock()
				return	
			}
			rf.becomeFollower(reply.Term) //另一种情况,同term,已投票
		} (peer, args) // tag go 线程变量
	}
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
	rand.Seed(time.Now().UnixNano()) // tag go 避免频繁调用

	rf := &Raft{
		peers: peers,	// mark lock no need
		persister: persister,	// mark lock no need
		me: me,
		currentTerm: 0,
		voteFor: -1,
		state: Follower,
		lastLogIndex: 0,
		commitIndex: 0,
		lastApplied: 0,
	}
	
	// Your initialization code here (2A, 2B, 2C).
	rf.log = append(rf.log, LogEntry{Term:rf.currentTerm}) // tag go
	// fmt.Printf("[node%d] len=%d\n", rf.me, len(rf.log))
	rf.ResetHbTimer()	

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("node%d init done\n", rf.me)
	// start ticker goroutine to start elections
	go rf.doElection()
	// go rf.DoHeartbeat()

	return rf
}
