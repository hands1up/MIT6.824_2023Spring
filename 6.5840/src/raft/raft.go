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
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"fmt"
	//	"6.824/labgob"
	"6.5840/labrpc"
)

type Status int//服务器扮演角色
type VoteState int//投票状态
type AppendEntriesState int//追加日志状态

const (
	Status_Leader		Status = 0//领导者
	Status_Follower		Status = 1//跟随者
	Status_Candidate	Status = 2//竞选者
)

const (
	ElectionTimeout		= time.Millisecond * 500
	RPCTimeout			= time.Millisecond * 100
	HeartBeatInterval	= time.Millisecond * 150

)

// const (
// 	VoteState_Normal	VoteState = 0//正常
// 	VoteState_Voted		VoteState = 1//已投票
// 	VoteState_Expire	VoteState = 2//已过期
// 	VoteState_Killed	VoteState = 3//已终止
// )

// const (
// 	AppendEntriesState_Normal	AppendEntriesState = 0//追加正常
// 	AppendEntriesState_Expire	AppendEntriesState = 1//追加过期
// 	AppendEntriesState_Killed	AppendEntriesState = 2//追加终止
// )
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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//data (2A)
	currentTerm			int//服务器已知最新的任期（在服务器首次启动的时候初始化为 0，单调递增）
	votedFor			int//当前任期内收到选票的候选者 id 如果没有投给任何候选者 则为空
	//logs				[]//日志条目；每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）

	commitIndex			int//已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied			int//已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex			[]int//对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex			[]int//对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）

	role				Status//该服务器角色
	electionTimer		*time.Timer
	appendEntriesTimers	[]*time.Timer



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.role == Status_Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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
	Term			int//候选人的任期号
	CandidateId		int//请求选票的候选人的 Id
	//LastLogIndex	int//候选人的最后日志条目的索引值
	//LastLogTerm		int//候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int//当前任期号，以便于候选人去更新自己的任期号
	VoteGranted	bool//候选人赢得了此张选票时为真
	//VoteState	VoteState//投票状态
}

type AppendEntriesArgs struct {
	Term			int//领导者的任期
	LeaderId		int//领导者ID 因此跟随者可以对客户端进行重定向（跟随者根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	//PrevLogIndex	int//紧邻新日志条目之前的那个日志条目的索引
	//PrevLogTerm		int//紧邻新日志条目之前的那个日志条目的任期
	//Entries			[]//需要被保存的日志条目（被当做心跳使用是 则日志条目内容为空；为了提高效率可能一次性发送多个）
	//LeaderCommit	int//领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term		int//当前任期，对于领导者而言它会更新自己的任期
	Success		bool//结果为真，如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm匹配上了
	//AppState 	AppendEntriesState//追加状态
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	term, isleader := rf.GetState()
	if term > args.Term {
		return
	} else if term == args.Term{
		if isleader == true {
			return
		}
		if rf.votedFor == args.CandidateId {
			reply.Term = args.Term
			reply.VoteGranted = true
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
	}

	if term < args.Term {
		rf.currentTerm = args.Term
		rf.role = Status_Follower
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}

	//todo logs

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.role = Status_Follower
	rf.resetElectionTimer()
	fmt.Printf("[	    func-RequestVote-rf(%+v)		] : rf.voted: %v\n", rf.me, rf.votedFor)


}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.role = Status_Follower
	rf.votedFor = -1
	rf.resetElectionTimer()
	reply.Success = true

	rf.mu.Unlock()
	return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if(server < 0 || server > len(rf.peers) || server == rf.me){
		panic("server invalid in sendRequestVote")
	}

	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i:=0;i<10&&!rf.killed();i++ {
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()
	
	select {
	case <- rpcTimer.C:
		//fmt.Printf("[	    func-sendRequestVote-rf(%+v)		] : rf.voted: %v rpcTimeout\n", rf.me, rf.votedFor)
		return
	case <- ch:
		return
	}
}

func (rf *Raft) AppendEntriesToPeer(peerId int) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.role != Status_Leader {
		rf.resetAppendEntriesTimer(peerId)
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:		rf.currentTerm,
		LeaderId:	rf.me,
	}
	reply := AppendEntriesReply{}
	rf.resetAppendEntriesTimer(peerId)
	rf.mu.Unlock()

	rf.sendAppendEntries(peerId, &args, &reply)

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.role = Status_Follower
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	if rf.role != Status_Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	ch := make(chan bool, 1)
	go func() {
		for i:=0;i<10&&!rf.killed();i++ {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
				continue
			} else {
				ch <- ok
				return
			}
		}
	}()

	select {
	case <- rpcTimer.C:
		return
	case <- ch:
		return
	}
	
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	go func() {

		for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
			select {
			case <- rf.electionTimer.C:
				rf.leaderElection()
			}
		}
	}()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(cur int) {
			for !rf.killed() {
				select {
				case <- rf.appendEntriesTimers[cur].C:
					rf.AppendEntriesToPeer(cur)
				}
			}
		}(i)
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	_, isleader := rf.GetState()
	if isleader {
		rf.mu.Unlock()
		return
	}

	rf.role = Status_Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	args := RequestVoteArgs {
		Term:			rf.currentTerm,
		CandidateId:	rf.me,
		//LastLogIndex
		//LastLogTerm
	}
	rf.mu.Unlock()

	peerNum := len(rf.peers)
	grantedNum := 1
	curNum := 1
	grantedChan := make(chan bool, peerNum-1)
	for i:=0;i<peerNum;i++ {
		if rf.me == i {
			continue
		}
		go func(gch chan bool, ind int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(ind, &args, &reply)
			gch <- reply.VoteGranted
			if args.Term < reply.Term {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.role = Status_Follower
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(grantedChan, i)
	} 

	for rf.role == Status_Candidate {
		flag := <- grantedChan
		curNum++
		if flag {
			grantedNum++
		}

		if grantedNum > peerNum/2 {
			rf.mu.Lock()
			if rf.role == Status_Candidate && rf.currentTerm == args.Term {
				rf.role = Status_Leader
				//todo log
				rf.resetElectionTimer()
			}
			if rf.role == Status_Leader {

			}
			rf.mu.Unlock()
		} else if curNum == peerNum || curNum - grantedNum > peerNum {
			return
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Status_Follower
	rf.electionTimer = time.NewTimer(rf.getElectionTimeout())

	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i:=0;i<len(rf.peers);i++ {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatInterval)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) getElectionTimeout() time.Duration {
	t := ElectionTimeout + time.Duration(rand.Int63()) % ElectionTimeout
	return t
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getElectionTimeout())
}

func (rf *Raft) resetAppendEntriesTimer(peerId int) {
	rf.appendEntriesTimers[peerId].Stop()
	rf.appendEntriesTimers[peerId].Reset(HeartBeatInterval)
}

