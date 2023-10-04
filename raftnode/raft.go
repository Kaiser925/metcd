package raftnode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"metcd/wait"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

const (
	internalTimeout    = time.Second
	readIndexRetryTime = 500 * time.Millisecond
)

// ProposePipe is a wrapper for the propose channel and error channel
// If ErrorC is not nil, raft propose process will be blocked until the error is read.
type ProposePipe struct {
	ProposeC chan string
	ErrorC   chan error
}

func (p *ProposePipe) Close() {
	close(p.ProposeC)
	if p.ErrorC != nil {
		close(p.ErrorC)
	}
}

type Commit struct {
	Data       []string
	ApplyDoneC chan<- struct{}
}

// RaftNode is a key-value stream backed by raft
type RaftNode struct {
	proposePipe *ProposePipe             // 用于接受数据变更提案, 并返回提案结果
	confChangeC <-chan raftpb.ConfChange // 用于接受集群配置变更的提案
	commitC     chan *Commit             // 确认记录 (k,v) 的日志项
	errorC      chan error               // raft 会话返回的错误

	id          int                    // raft 会话中的客户端 ID
	peers       []string               // raft peer 的 url
	join        bool                   // 标志节点是加入一个已经存在的集群
	waldir      string                 // 存放 WAL 日志的目录
	snapdir     string                 // 存放快照的目录
	getSnapshot func() ([]byte, error) // 获取快照的方法

	leaderChanged *Notifier // leaderChanged is used to notify the linearizable read loop to drop the old read requests.

	applyWait wait.WaitTime

	readMu sync.RWMutex
	// read routine notifies that it waits for reading by sending an empty struct to
	readwaitc chan struct{}
	// readNotifier is used to Notify the read routine that it can process the request
	// when there is no error
	readNotifier *ErrorNotifier

	readStateC chan raft.ReadState
	idGen      *Generator

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	lead          uint64 // 当前集群的 Leader ID

	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // 通知 Snapshotter 已经就绪了

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

var DefaultSnapshotCount uint64 = 10000

// NewRaftNode 实例化 RaftNode, 并开始运行实例. 通过关闭 ProposePipe.ProposeC 来停止实例
// 通过 CommitC(), ErrorC(), SnapshotterReady() 获取提交的日志, 错误以及快照就绪信息.
func NewRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposePipe *ProposePipe,
	confChangeC <-chan raftpb.ConfChange) *RaftNode {

	commitC := make(chan *Commit)
	errorC := make(chan error)

	rc := &RaftNode{
		proposePipe:   proposePipe,
		confChangeC:   confChangeC,
		commitC:       commitC,
		errorC:        errorC,
		id:            id,
		peers:         peers,
		join:          join,
		waldir:        fmt.Sprintf("metcd-%d", id),
		snapdir:       fmt.Sprintf("metcd-%d-snap", id),
		getSnapshot:   getSnapshot,
		snapCount:     DefaultSnapshotCount,
		stopc:         make(chan struct{}),
		httpstopc:     make(chan struct{}),
		httpdonec:     make(chan struct{}),
		leaderChanged: NewNotifier(),
		readNotifier:  NewErrorNotifier(),
		readwaitc:     make(chan struct{}, 1),
		applyWait:     wait.NewTimeList(),
		readStateC:    make(chan raft.ReadState, 1),
		idGen:         NewGenerator(uint16(id), time.Now()),

		logger: zap.NewExample(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return rc
}

// CommitC 返回一个 channel, 用于接收已提交的日志条目
func (rc *RaftNode) CommitC() <-chan *Commit {
	return rc.commitC
}

// ErrorC 返回一个 channel, 用于接收错误信息
func (rc *RaftNode) ErrorC() <-chan error {
	return rc.errorC
}

// SnapshotterReady 返回一个 channel, 用于接收快照就绪信息
func (rc *RaftNode) SnapshotterReady() <-chan *snap.Snapshotter {
	return rc.snapshotterReady
}

func (rc *RaftNode) ID() uint64 {
	return uint64(rc.id)
}

func (rc *RaftNode) LeaderID() uint64 {
	return rc.getLead()
}

func (rc *RaftNode) IsLeader() bool {
	return rc.getLead() == uint64(rc.id)
}

func (rc *RaftNode) setLead(v uint64) {
	atomic.StoreUint64(&rc.lead, v)
}

func (rc *RaftNode) getLead() uint64 {
	return atomic.LoadUint64(&rc.lead)
}

func (rc *RaftNode) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&rc.appliedIndex, v)
}

func (rc *RaftNode) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&rc.appliedIndex)
}

func (rc *RaftNode) getSnapshotIndex() uint64 {
	return atomic.LoadUint64(&rc.snapshotIndex)
}

func (rc *RaftNode) setSnapshotIndex(v uint64) {
	atomic.StoreUint64(&rc.snapshotIndex, v)
}

func (rc *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}
	// 在写入 WAL 前保存快照, 可能会导致孤儿快照, 但是避免了日志项存在快照记录
	// 实际没有快照文件的情况.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.getAppliedIndex()+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.getAppliedIndex())
	}
	if rc.getAppliedIndex()-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.getAppliedIndex()-firstIdx+1:]
	}
	return nents
}

// publishEntries 将已提交的日志条目写入 Commit 通道,并返回是否能发布所有的条目
func (rc *RaftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &Commit{data, applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after Commit, update appliedIndex
	rc.setAppliedIndex(ents[len(ents)-1].Index)
	rc.applyWait.Trigger(rc.getAppliedIndex())

	return applyDoneC, true
}

func (rc *RaftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			panic(fmt.Sprintf("listing snapshots (%v)", err))
		}
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			panic(fmt.Sprintf("loading snapshots (%v)", err))
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL 返回一个用于读取的 WAL
func (rc *RaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("metcd:cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("metcd:create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("metcd:error loading wal (%v)", err)
	}

	return w
}

// replayWAL 重放日志到 raft 实例
func (rc *RaftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("metcd:failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)

	return w
}

func (rc *RaftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *RaftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("metcd:cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	// signal replay has finished
	rc.snapshotterReady <- rc.snapshotter

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()
	go rc.linearizableReadLoop()
}

// stop closes http, closes all channels, and stops raft.
func (rc *RaftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *RaftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.getAppliedIndex() {
		panic(fmt.Sprintf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.getAppliedIndex()))
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.setSnapshotIndex(snapshotToSave.Metadata.Index)
	rc.setAppliedIndex(snapshotToSave.Metadata.Index)
}

var SnapshotCatchUpEntriesN uint64 = 10000

func (rc *RaftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	appliedIndex, snapshotIndex := rc.getAppliedIndex(), rc.getSnapshotIndex()
	if appliedIndex-snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", appliedIndex, snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if appliedIndex > SnapshotCatchUpEntriesN {
		compactIndex = appliedIndex - SnapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		if err != raft.ErrCompacted {
			panic(err)
		}
	} else {
		log.Printf("compacted log at index %d", compactIndex)
	}

	rc.setSnapshotIndex(appliedIndex)
}

func (rc *RaftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.setSnapshotIndex(snap.Metadata.Index)
	rc.setAppliedIndex(snap.Metadata.Index)

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposePipe.ProposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposePipe.ProposeC:
				if !ok {
					rc.proposePipe.ProposeC = nil
				} else {
					// 阻塞直到 raft 状态机接受提案
					err := rc.node.Propose(context.TODO(), []byte(prop))
					if rc.proposePipe.ErrorC != nil {
						rc.proposePipe.ErrorC <- err
					}
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// 处理 raft 状态机的更新事件
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// 将 raft 的 entries 写入 wal，然后通过 Commit channel 发布
		case rd := <-rc.node.Ready():
			// 发送了当前的状态信息
			if rd.SoftState != nil {
				newLeader := rd.SoftState.Lead != raft.None && rc.getLead() != rd.SoftState.Lead
				if newLeader {
					rc.setLead(rd.SoftState.Lead)
					rc.leaderChanged.Notify() // 通知 leader 发生变更
				}
			}

			if len(rd.ReadStates) != 0 {
				select {
				case rc.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(internalTimeout):
					rc.logger.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
				case <-rc.stopc:
					return
				}
			}

			// 正常的数据处理

			// 保存 snapshot 和 wal snapshot entry，然后再保存其他 entries 和 hardstate
			// 以确保在 snapshot 恢复后可以恢复
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
			}
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rc.processMessages(rd.Messages))
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot(applyDoneC)
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

// 如果在创建快照之后有一个 “raftpb.EntryConfChange时“,
// 快照中包含的 confState 就是过时的。
// 所以在给 follower 发送快照之前,需要更新 confState.
func (rc *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}

func (rc *RaftNode) LinearizableReadNotify(ctx context.Context) error {
	return rc.linearizableReadNotify(ctx)
}

func (rc *RaftNode) linearizableReadNotify(ctx context.Context) error {
	rc.readMu.RLock()
	nc := rc.readNotifier
	rc.readMu.RUnlock()

	// signal linearizable loop for current Notify if it hasn't been already
	select {
	case rc.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.Receive():
		return nc.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.httpdonec:
		return ErrStopped
	}
}

func uint64ToBigEndianBytes(number uint64) []byte {
	byteResult := make([]byte, 8)
	binary.BigEndian.PutUint64(byteResult, number)
	return byteResult
}

func (rc *RaftNode) linearizableReadLoop() {
	for {
		leaderChangedNotifier := rc.leaderChanged.Receive()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-rc.readwaitc:
		case <-rc.stopc:
			return
		}

		nextnr := NewErrorNotifier()
		rc.readMu.Lock()
		nr := rc.readNotifier
		rc.readNotifier = nextnr
		rc.readMu.Unlock()
		confirmedIndex, err := rc.requestCurrentIndex()
		if err != nil {
			nr.Notify(err)
			continue
		}

		appliedIndex := rc.getAppliedIndex()

		if appliedIndex < confirmedIndex {
			select {
			case <-rc.applyWait.Wait(confirmedIndex): // 阻塞线性读, 直到追上 confirmedIndex
			case <-rc.stopc:
				return
			}
		}

		nr.Notify(nil)
	}
}

func (rc *RaftNode) requestCurrentIndex() (uint64, error) {
	requestId := rc.idGen.Next()
	err := rc.sendReadIndex(requestId)
	if err != nil {
		return 0, err
	}

	errorTimer := time.NewTimer(5 * time.Second)
	defer errorTimer.Stop()
	retryTimer := time.NewTimer(readIndexRetryTime)
	defer retryTimer.Stop()

	for {
		select {
		case rs := <-rc.readStateC: // 读取集群响应的结果
			requestIdBytes := uint64ToBigEndianBytes(requestId)
			gotOwnResponse := bytes.Equal(rs.RequestCtx, requestIdBytes)
			if !gotOwnResponse { // 不是请求的响应, 忽略
				// a previous request might time out. now we should ignore the response of it and
				// continue waiting for the response of the current requests.
				//responseId := uint64(0)
				//if len(rs.RequestCtx) == 8 {
				//	responseId = binary.BigEndian.Uint64(rs.RequestCtx)
				//}
				//
				// TODO:(bobby): log slow read requests
				continue
			}
			return rs.Index, nil
		case <-rc.leaderChanged.Receive(): // 集群 Leader 发生变更, 需要重新请求
			return 0, ErrLeaderChanged
		case <-errorTimer.C: // 超时了
			return 0, ErrTimeout
		case <-retryTimer.C: // 尝试重新请求
			if err := rc.sendReadIndex(requestId); err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
			continue
		case <-rc.stopc:
			return 0, ErrStopped
		}
	}
}

func (rc *RaftNode) sendReadIndex(requestId uint64) error {
	ctxToSend := uint64ToBigEndianBytes(requestId)

	cctx, cancel := context.WithTimeout(context.Background(), internalTimeout)
	err := rc.node.ReadIndex(cctx, ctxToSend)
	cancel()
	if errors.Is(err, raft.ErrStopped) {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

func (rc *RaftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("metcd:Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("metcd:Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("metcd:Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *RaftNode) IsIDRemoved(_ uint64) bool   { return false }
func (rc *RaftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
