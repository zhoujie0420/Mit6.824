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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824-golabs-2020/src/labrpc"
)

// some constants

// status 节点的角色
type Status int

// VoteState 投票的状态
type VoteState int

// AppendEntries 追加日志的状态
type AppendEntriesState int

// HeartBeatTimeout 心跳超时时间 全局
var HeartBeatTimeout = 120 * time.Millisecond

// 枚举节点类型：跟随者，竞选者，领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota //投票过程正常
	Killed                  // Raft节点已终止
	Expire                  // 投票（消息/竞选者）过期
	Voted                   // 本Term已投票

)

const (
	AppNormal    AppendEntriesState = iota //追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              //Raft程序终止
	AppRepeat                              //追加重复  2B
	AppCommited                            //追加的日志已提交  2B
	Mismatch                               // 追加不匹配  2B
)

// LogEntry log条目
type LogEntry struct {
	Term    int         // leader收到日志条目的任期
	Command interface{} // 状态机执行的指令
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D
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

	// 所有的servers拥有的变量:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	// 所有的servers经常修改的:
	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值

	// leader拥有的可见变量，用来管理他的follower(leader经常修改的）
	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	// 由自己追加的:
	status   Status        // 该节点是什么角色（状态）
	overtime time.Duration // 设置超时时间，200-400ms
	timer    *time.Ticker  // 每个节点中的计时器

	applyChan chan ApplyMsg // 日志都是存在这里client取（2B）
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 需要竞选的人的任期
	CandidateId  int // 需要竞选的人的id
	LastLogIndex int // 竞选人日志条目的最后索引
	LastLogTerm  int // 候选人最后日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool      // 是否投票给了该竞选人
	VoteState   VoteState // 投票状态
}

// AppendEntriesArgs 由leader复制log条目，也可以当做是心跳连接，注释中的rf为leader节点
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 预计要从哪里追加的index，因此每次要比当前的len(logs)多1 args初始化为：rf.nextIndex[i] - 1
	PrevLogTerm  int        // 追加新的日志的任期号(这边传的应该都是当前leader的任期号 args初始化为：rf.currentTerm
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term     int                // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success  bool               //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	AppState AppendEntriesState // 追加状态
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//对应论文中的初始化
	rf.applyChan = applyCh // 2B
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms
	rf.timer = time.NewTicker(rf.overtime)

	// 从崩溃前持续的状态初始化
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//开始 ticker goroutine 开始选举
	go rf.ticker()

	return rf
}

// raft的ticker(心脏)
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

// 此外其有若干个后台协程，它们分别是：

// ticker：共 1 个，用来触发 heartbeat timeout 和 election timeout
// applier：共 1 个，用来往 applyCh 中 push 提交的日志并保证 exactly once
// replicator：共 len(peer) - 1 个，用来分别管理对应 peer 的复制状态

// ticker 协程会定期收到两个 timer 的到期事件，如果是 election timer 到期，则发起一轮选举；如果是 heartbeat timer 到期且节点是 leader，则发起一轮心跳。

// 初始化的时候给每个timer设了一个overtime，并通过select监听，时间一过就根据其当前节点的status去进行选举/日志同步。
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// 您的代码在这里检查领导人选举是否应该
		// 开始并随机使用睡眠时间

		//当定时器结束进行超时选举
		select {

		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()

			// 根据自身的status进行一次ticker
			switch rf.status {
			// follower 变成竞选者
			case Follower:
				rf.status = Candidate
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1 //统计自身的票数

				// 每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200 - 400 ms
				rf.timer.Reset(rf.overtime)

				// 对自身以外的节点进行选举
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  0,
					}

					if len(rf.logs) > 0 {
						voteArgs.LastLogTerm = rf.logs[len(rf.logs)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			case Leader:
				// 进行心跳/日志同步
				appendNums := 1 //对正确的返回的节点数量
				rf.timer.Reset(HeartBeatTimeout)
				// 构造msg
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}

					appendEntriesReply := AppendEntriesReply{}

					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
				}
			}
			rf.mu.Unlock()
		}
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

// 投票RPC实现
//首先竞选者的任期必须，大于自己的任期。否则返回false。因为在出先网络分区时，可能两个分区分别产生了两个leader。
//那么我们认为应该是任期长的leader拥有的数据更完整。（在可视化中也有）。
//因此第二条就是，投票成功的前提的是，你自己的票要没投过给别人，且竞选者的日志状态要和你的一致。
//还有一个要注意的点是当前rf节点发送到别的rf的节点的RPC框架中已经给出，为sendRequestVote方法。
//如果原本自身是follower的话那么如果超时，就变成竞选者。对每个节点发送RPC投票请求。

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNums *int) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 由于网络分区，请求投票的人的term的比自己的还小，不给予投票
	if args.Term < rf.currentTerm {
		return false
	}

	// 对reply的返回情况进行分支处理
	switch reply.VoteState {
	// 消息过期的两种情况：
	// 1. 本身的term过期了比节点还小
	// 2. 节点日志的条目落后于节点
	case Expire:
		{
			rf.status = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		// 根据是否同意投票，收集选票数量
		if reply.VoteGranted && reply.Term == rf.currentTerm && *voteNums <= len(rf.peers)/2 {
			*voteNums++
		}
		// 票数超过一半
		if *voteNums >= len(rf.peers)/2 {

			*voteNums = 0

			//本身就是leader在网络分区中更有话语权的leader
			if rf.status == Leader {
				return ok
			}

			// 本身不是leader，那么需要初始化nextIndex数组
			rf.status = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs) + 1
			}
			rf.timer.Reset(HeartBeatTimeout)
		}

	case Killed:
		return false
	}
	return ok
}

// example RequestVote RPC handler.

// handler 需要注意的是只有在 grant 投票时才重置选举超时时间，这样有助于网络不稳定条件下选主的 liveness 问题，这一点 guidance 里面有介绍到。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前节点crash
	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	//reason: 出现网络分区，该竞选者已经OutOfDate(过时）
	if args.Term < rf.currentTerm {
		reply.VoteState = Expire
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		//重置自身状态
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// 此时比自己任期小的都已经把票还原
	if rf.votedFor == -1 {

		currentLogIndex := len(rf.logs) - 1
		currentLogTerm := 0
		// 如果currentLogIndex下标不是-1就把term赋值过来
		if currentLogIndex >= 0 {
			currentLogTerm = rf.logs[currentLogIndex].Term
		}
		//  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		// 论文里的第二个匹配条件，当前peer要符合arg两个参数的预期
		if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
			reply.VoteState = Expire
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		//给票数 并返回true
		rf.votedFor = args.CandidateId

		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.timer.Reset(rf.overtime)

	} else {
		// 只剩下人气想用 但是票已经给了，此时存在两种情况
		reply.VoteState = Voted
		reply.VoteGranted = false

		// 1. 当前节点来自同一轮，不同竞选者，但是票数以及给了，或者它本身就是竞选者
		if rf.votedFor != args.CandidateId {
			// 告诉reply 票已经没了 返回false
			return
		} else {
			// 2.当前的节点票已经给了同一个人，但是由于sleep的原因，还没来得及更新状态，又发了一次请求
			// 重置自身状态
			rf.status = Follower
		}

		rf.timer.Reset(rf.overtime)

	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) bool {

	if rf.killed() {
		return false
	}

	// 如果append失败，应该不断的retries ，直到log被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	// 必须加在这里否则加载前面reptry时进入，rpc也需要一个锁，但又获取不到，因为锁已经被加上了

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对reply的返回状态进行分支
	switch reply.AppState {

	//目标节点crash
	case AppKilled:
		{
			return false
		}
	case AppNormal:
		{
			// 2A test的目的是让leader能不能连续任期，所以2A只需要对节点初始化然后返回
			return true
		}

	// reason: 出现网络分区，该leader已经OutOfDate(过时）
	case AppOutOfDate:
		//该节点变成追随者 ，并重置rf 状态
		rf.status = Follower
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
		rf.currentTerm = reply.Term
	}
	return ok
}

// AppendEntries 建立心跳，日志同步RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 节点crash
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}
	// 出现网络分区，args的任期 比当前的raft的任期还要小，说明args之前所在的分区已经outofdate
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// 对当前的rf 进行ticker重置
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.status = Follower
	rf.timer.Reset(rf.overtime)

	// 对返回的reply进行赋值
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock() // 保证锁一定会被释放
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm

	if rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
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

	return index, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

// 利用killed可以查看是否调用过kill
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
