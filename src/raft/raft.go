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
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state //note
	peers     []*labrpc.ClientEnd // RPC end points of all peers // todo // tag rpc
	persister *Persister          // Object to hold this peer's persisted state //todo
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill() //todo

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries //note
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries //note //todo why*
	state          NodeState

	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command
	// note 至少有一条entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == StateLeader
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) { // note 底层[]byte
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data) // mark 解析三步
	d := labgob.NewDecoder(r)  // 2
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil { // 3
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	// there will always be at least one entry in rf.logs // note
	rf.lastApplied, rf.commitIndex = rf.logs[0].Index, rf.logs[0].Index // note
}

func (rf *Raft) encodeState() []byte { // tag os
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex { // mark
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	// 压缩log
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]Entry, 1) // 丢弃整个log
	} else {
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]) //
		rf.logs[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex // note 设置第一个log
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex // note

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot) // 持久化
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) { // todo snapshot哪来
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs = shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil // note
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

func (rf *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // mark persist, 先释放锁 因为persist里有锁
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) { // mark 本轮已投过票
		response.Term, response.VoteGranted = rf.currentTerm, false // mark 失败, receiver状态不变, 返回更新的term
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(StateFollower)
		rf.currentTerm, rf.votedFor = request.Term, -1 // mark 1.更新term,切换follwer,指向的leader失效
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		response.Term, response.VoteGranted = rf.currentTerm, false // mark log更旧, 不投票
		return
	}
	rf.votedFor = request.CandidateId                          // mark grant
	rf.electionTimer.Reset(RandomizedElectionTimeout())        // mark 投票 timeout重置 // tag go Timer.Reset
	response.Term, response.VoteGranted = rf.currentTerm, true // mark currentTerm
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response) // tag go debug log

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.ChangeState(StateFollower)                       // test 必须的吗
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // 重置

	if request.PrevLogIndex < rf.getFirstLog().Index { // todo lab4
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) { // 检查2
		response.Term, response.Success = rf.currentTerm, false // 2
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex { // prevLogIndex指向log之外
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1 //note
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index // mark 找ConflictTerm的起始index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries { // 4 append
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term { // note append不存在的entry, 找分界点, 第二种case 已包含部分entry // test 基于prevLogIndex直接拼接, 不这样做 因为gc问题吗
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...)) // tag os
			break
		}
	}

	rf.advanceCommitIndexForFollower(request.LeaderCommit) // 5 commit

	response.Term, response.Success = rf.currentTerm, true
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) { // handler
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm { // check term
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.persist()
	}

	rf.ChangeState(StateFollower)
	rf.electionTimer.Reset(RandomizedElectionTimeout()) // mark

	// outdated snapshot
	if request.LastIncludedIndex <= rf.commitIndex { // note 过时的快照
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

// example code to send a RPC to a server.
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
func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, response *RequestVoteResponse) bool {
	return rf.peers[server].Call("Raft.RequestVote", request, response) // tag rpc .
}

func (rf *Raft) sendAppendEntries(server int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	return rf.peers[server].Call("Raft.AppendEntries", request, response)
}

func (rf *Raft) sendInstallSnapshot(server int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", request, response)
}

func (rf *Raft) StartElection() { // mark ↳fig2 candidate
	request := rf.genRequestVoteRequest() // note 只读,可共用
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1 // note
	rf.votedFor = rf.me
	rf.persist() // mark 有变化,刷盘
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) { // tag 并发 // tag go 传参数 协程拥有
			response := new(RequestVoteResponse)
			if rf.sendRequestVote(peer, request, response) { // note send
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == StateCandidate { // mark 检查过期rpc
					if response.VoteGranted { // mark grant
						grantedVotes += 1                   // todo 闭包? // tag go
						if grantedVotes > len(rf.peers)/2 { // note 检查过半
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(StateLeader)
							rf.BroadcastHeartbeat(true) // mark heartbeat
						}
					} else if response.Term > rf.currentTerm { // note 无获得选票,且更大的term
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(StateFollower)
						rf.currentTerm, rf.votedFor = response.Term, -1 // mark 失败,清除voteFor
						rf.persist()                                    // note 变量有变, 更新
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer) // mark 立刻, 因为需要维护leadership
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal() // mark 发信号至replicator, 等待rpc, 批量发送entry(可能条件变量的方式 有一定时间滞后) // tag go
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock() // tag go
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index { // mark log同步落后, 这部分entry已压缩到snapshot, 只能从snapshot获取
		// only snapshot can catch up
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response) // todo
			rf.mu.Unlock()
		}
	} else {
		// just entries can catch up
		request := rf.genAppendEntriesRequest(prevLogIndex) // note 打包发送
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) { // tag rpc 成功
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response) // todo
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteRequest {
	lastLog := rf.getLastLog()
	return &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	firstIndex := rf.getFirstLog().Index
	entries := make([]Entry, len(rf.logs[prevLogIndex+1-firstIndex:]))
	copy(entries, rf.logs[prevLogIndex+1-firstIndex:]) // tag os
	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstIndex].Term, // mark
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries) // note
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1                      // note
			rf.advanceCommitIndexForLeader()                                  // commit
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(StateFollower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex // note 回退nextIndex
				if response.ConflictTerm != -1 {            // entry follower中存在, 但不一致
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm { // note ConflictTerm结束位置
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling AppendEntriesResponse %v for AppendEntriesRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	firstLog := rf.getFirstLog()
	return &InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {
	if rf.state == StateLeader && rf.currentTerm == request.Term {
		if response.Term > rf.currentTerm {
			rf.ChangeState(StateFollower)
			rf.currentTerm, rf.votedFor = response.Term, -1
			rf.persist()
		} else {
			rf.matchIndex[peer], rf.nextIndex[peer] = request.LastIncludedIndex, request.LastIncludedIndex+1
		}
	}
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after handling InstallSnapshotResponse %v for InstallSnapshotRequest %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), response, request)
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %d} changes state from %s to %s in term %d", rf.me, rf.state, state, rf.currentTerm)
	rf.state = state
	switch state {
	case StateFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	case StateCandidate:
	case StateLeader:
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// used to compute and advance commitIndex by matchIndex[]
func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	insertionSort(srt)
	newCommitIndex := srt[n-(n/2+1)] // todo
	if newCommitIndex > rf.commitIndex {
		// only advance commitIndex for current term's log
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			DPrintf("{Node %d} advance commitIndex from %d to %d with matchIndex %v in term %d", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			DPrintf("{Node %d} can not advance commitIndex from %d because the term of newCommitIndex %d is not equal to currentTerm %d", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
		}
	}
}

// used to advance commitIndex by leaderCommit
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %d} advance commitIndex from %d to %d with leaderCommit %d in term %d", rf.me, rf.commitIndex, newCommitIndex, leaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

// used by RequestVote Handler to judge which log is newer
func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// used by AppendEntries Handler to judge whether log is matched
func (rf *Raft) matchLog(term, index int) bool {
	return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstLog().Index].Term == term // note 2
}

// used by Start function to append a new Entry to logs
func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLog := rf.getLastLog()
	newLog := Entry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLog.Index, newLog.Index+1
	rf.persist()
	return newLog
}

// used by replicator goroutine to judge whether a peer needs replicating
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index // note
}

// used by upper layer to detect whether there are any logs in current term
func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastLog().Term == rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command) // leader加log
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false) // 异步
	return newLog.Index, newLog.Term, true
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
	atomic.StoreInt32(&rf.dead, 1) // todo 原子
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) Me() int {
	return rf.me
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select { // tag go
		case <-rf.electionTimer.C: // tag go Timer
			rf.mu.Lock()
			rf.ChangeState(StateCandidate)
			rf.currentTerm += 1
			rf.StartElection()                                  // ↳fig2 candidates
			rf.electionTimer.Reset(RandomizedElectionTimeout()) // mark 重置计时器, 重选时间
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == StateLeader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// a dedicated applier goroutine to guarantee that each log will be push into applyCh exactly once, ensuring that service's applying entries and raft's committing entries can be parallel
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex { // note 触发条件
			rf.applyCond.Wait() // note wait
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock() // tag 并发 push channel前释放锁
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{ // note push log
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = Max(rf.lastApplied, commitIndex) // note 根据commitIndex更新rf.lastApplied
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock() // tag go
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false { // note for loop
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait() // mark 释放CPU,等信号
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer) // 多次
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
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // todo
	rf.applyCond = sync.NewCond(&rf.mu)       // todo api
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i) // todo
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker() // todo
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier() // todo

	return rf
}
