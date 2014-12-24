// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcnode

import (
	"container/list"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hlandauf/btcchain"
	"github.com/hlandauf/btcdb"
	"github.com/hlandauf/btcnet"
	"github.com/hlandauf/btcutil"
	"github.com/hlandauf/btcwire"
)

const (
	chanBufferSize = 50

	// minInFlightBlocks is the minimum number of blocks that should be
	// in the request queue for headers-first mode before requesting
	// more.
	minInFlightBlocks = 10

	// blockDbNamePrefix is the prefix for the block database name.  The
	// database type is appended to this value to form the full block
	// database name.
	blockDbNamePrefix = "blocks"

	// namesDbNamePrefix is the prefix for the name database name.
	nameDbNamePrefix = "names"
)

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer *Peer
}

// blockMsg packages a bitcoin block message and the peer it came from together
// so the block handler has access to that information.
type blockMsg struct {
	block *btcutil.Block
	peer  *Peer
}

// invMsg packages a bitcoin inv message and the peer it came from together
// so the block handler has access to that information.
type invMsg struct {
	inv  *btcwire.MsgInv
	peer *Peer
}

// blockMsg packages a bitcoin block message and the peer it came from together
// so the block handler has access to that information.
type headersMsg struct {
	headers *btcwire.MsgHeaders
	peer    *Peer
}

// donePeerMsg signifies a newly disconnected peer to the block handler.
type donePeerMsg struct {
	peer *Peer
}

// txMsg packages a bitcoin tx message and the peer it came from together
// so the block handler has access to that information.
type txMsg struct {
	tx   *btcutil.Tx
	peer *Peer
}

// getSyncPeerMsg is a message type to be sent across the message channel for
// retrieving the current sync peer.
type getSyncPeerMsg struct {
	reply chan *Peer
}

// checkConnectBlockMsg is a message type to be sent across the message channel
// for requesting chain to check if a block connects to the end of the current
// main chain.
type checkConnectBlockMsg struct {
	block *btcutil.Block
	reply chan error
}

// calcNextReqDifficultyResponse is a response sent to the reply channel of a
// calcNextReqDifficultyMsg query.
type calcNextReqDifficultyResponse struct {
	difficulty uint32
	err        error
}

// calcNextReqDifficultyMsg is a message type to be sent across the message
// channel for requesting the required difficulty of the next block.
type calcNextReqDifficultyMsg struct {
	timestamp time.Time
	reply     chan calcNextReqDifficultyResponse
}

// processBlockResponse is a response sent to the reply channel of a
// processBlockMsg.
type processBlockResponse struct {
	isOrphan bool
	err      error
}

// processBlockMsg is a message type to be sent across the message channel
// for requested a block is processed.  Note this call differs from blockMsg
// above in that blockMsg is intended for blocks that came from peers and have
// extra handling whereas this message essentially is just a concurrent safe
// way to call ProcessBlock on the internal block chain instance.
type processBlockMsg struct {
	block *btcutil.Block
	flags btcchain.BehaviorFlags
	reply chan processBlockResponse
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the block manager believes it is synced with
// the currently connected peers.
type isCurrentMsg struct {
	reply chan bool
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height int64
	sha    *btcwire.ShaHash
}

// chainState tracks the state of the best chain as blocks are inserted.  This
// is done because btcchain is currently not safe for concurrent access and the
// block manager is typically quite busy processing block and inventory.
// Therefore, requesting this information from chain through the block manager
// would not be anywhere near as efficient as simply updating it as each block
// is inserted and protecting it with a mutex.
type ChainState struct {
	m                 sync.Mutex
	NewestHash        *btcwire.ShaHash
	NewestHeight      int64
	PastMedianTime    time.Time
	PastMedianTimeErr error
}

// Best returns the block hash and height known for the tip of the best known
// chain.
//
// This function is safe for concurrent access.
func (c *ChainState) Best() (*btcwire.ShaHash, int64) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.NewestHash, c.NewestHeight
}

// blockManager provides a concurrency safe block manager for handling all
// incoming blocks.
type blockManager struct {
	node              *Node
	started           int32
	shutdown          int32
	blockChain        *btcchain.BlockChain
	requestedTxns     map[btcwire.ShaHash]struct{}
	requestedBlocks   map[btcwire.ShaHash]struct{}
	receivedLogBlocks int64
	receivedLogTx     int64
	lastBlockLogTime  time.Time
	processingReqs    bool
	syncPeer          *Peer
	msgChan           chan interface{}
	chainState        ChainState
	wg                sync.WaitGroup
	quit              chan struct{}

	// The following fields are used for headers-first mode.
	headersFirstMode bool
	headerList       *list.List
	startHeader      *list.Element
	nextCheckpoint   *btcnet.Checkpoint

	NotifyBlockConnected    notifyBlockConnectedFunc
	NotifyBlockConnectedGBT notifyBlockConnectedGBTFunc
	NotifyBlockDisconnected notifyBlockDisconnectedFunc
}

type notifyBlockConnectedFunc func(block *btcutil.Block)
type notifyBlockConnectedGBTFunc func(blockSha *btcwire.ShaHash)
type notifyBlockDisconnectedFunc func(block *btcutil.Block)

// resetHeaderState sets the headers-first mode state to values appropriate for
// syncing from a new peer.
func (b *blockManager) resetHeaderState(newestHash *btcwire.ShaHash, newestHeight int64) {
	b.headersFirstMode = false
	b.headerList.Init()
	b.startHeader = nil

	// When there is a next checkpoint, add an entry for the latest known
	// block into the header pool.  This allows the next downloaded header
	// to prove it links to the chain properly.
	if b.nextCheckpoint != nil {
		node := headerNode{height: newestHeight, sha: newestHash}
		b.headerList.PushBack(&node)
	}
}

// updateChainState updates the chain state associated with the block manager.
// This allows fast access to chain information since btcchain is currently not
// safe for concurrent access and the block manager is typically quite busy
// processing block and inventory.
func (b *blockManager) updateChainState(newestHash *btcwire.ShaHash, newestHeight int64) {
	b.chainState.m.Lock()
	defer b.chainState.m.Unlock()

	b.chainState.NewestHash = newestHash
	b.chainState.NewestHeight = newestHeight
	medianTime, err := b.blockChain.CalcPastMedianTime()
	if err != nil {
		b.chainState.PastMedianTimeErr = err
	} else {
		b.chainState.PastMedianTime = medianTime
	}
}

// findNextHeaderCheckpoint returns the next checkpoint after the passed height.
// It returns nil when there is not one either because the height is already
// later than the final checkpoint or some other reason such as disabled
// checkpoints.
func (b *blockManager) findNextHeaderCheckpoint(height int64) *btcnet.Checkpoint {
	// There is no next checkpoint if checkpoints are disabled or there are
	// none for this current network.
	if b.node.cfg.DisableCheckpoints {
		return nil
	}
	checkpoints := b.node.cfg.ActiveNetParams.Checkpoints
	if len(checkpoints) == 0 {
		return nil
	}

	// There is no next checkpoint if the height is already after the final
	// checkpoint.
	finalCheckpoint := &checkpoints[len(checkpoints)-1]
	if height >= finalCheckpoint.Height {
		return nil
	}

	// Find the next checkpoint.
	nextCheckpoint := finalCheckpoint
	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}
		nextCheckpoint = &checkpoints[i]
	}
	return nextCheckpoint
}

// startSync will choose the best peer among the available candidate peers to
// download/sync the blockchain from.  When syncing is already running, it
// simply returns.  It also examines the candidates for any which are no longer
// candidates and removes them as needed.
func (b *blockManager) startSync(peers *list.List) {
	// Return now if we're already syncing.
	if b.syncPeer != nil {
		return
	}

	// Find the height of the current known best block.
	_, height, err := b.node.db.NewestSha()
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	var bestPeer *Peer
	var enext *list.Element
	for e := peers.Front(); e != nil; e = enext {
		enext = e.Next()
		p := e.Value.(*Peer)

		// Remove sync candidate peers that are no longer candidates due
		// to passing their latest known block.  NOTE: The < is
		// intentional as opposed to <=.  While techcnically the peer
		// doesn't have a later block when it's equal, it will likely
		// have one soon so it is a reasonable choice.  It also allows
		// the case where both are at 0 such as during regression test.
		if p.lastBlock < int32(height) {
			peers.Remove(e)
			continue
		}

		// TODO(davec): Use a better algorithm to choose the best peer.
		// For now, just pick the first available candidate.
		bestPeer = p
	}

	// Start syncing from the best peer if one was selected.
	if bestPeer != nil {
		locator, err := b.blockChain.LatestBlockLocator()
		if err != nil {
			log.Errorf("Failed to get block locator for the "+
				"latest block: %v", err)
			return
		}

		log.Infof("Syncing to block height %d from peer %v",
			bestPeer.lastBlock, bestPeer.addr)

		// When the current height is less than a known checkpoint we
		// can use block headers to learn about which blocks comprise
		// the chain up to the checkpoint and perform less validation
		// for them.  This is possible since each header contains the
		// hash of the previous header and a merkle root.  Therefore if
		// we validate all of the received headers link together
		// properly and the checkpoint hashes match, we can be sure the
		// hashes for the blocks in between are accurate.  Further, once
		// the full blocks are downloaded, the merkle root is computed
		// and compared against the value in the header which proves the
		// full block hasn't been tampered with.
		//
		// Once we have passed the final checkpoint, or checkpoints are
		// disabled, use standard inv messages learn about the blocks
		// and fully validate them.  Finally, regression test mode does
		// not support the headers-first approach so do normal block
		// downloads when in regression test mode.
		if b.nextCheckpoint != nil && height < b.nextCheckpoint.Height &&
			!b.node.cfg.RegressionTest && !b.node.cfg.DisableCheckpoints {

			bestPeer.xPushGetHeadersMsg(locator, b.nextCheckpoint.Hash)
			b.headersFirstMode = true
			log.Infof("Downloading headers for blocks %d to "+
				"%d from peer %s", height+1,
				b.nextCheckpoint.Height, bestPeer.addr)
		} else {
			bestPeer.xPushGetBlocksMsg(locator, &zeroHash)
		}
		b.syncPeer = bestPeer
	} else {
		log.Warnf("No sync peer candidates available")
	}
}

// isSyncCandidate returns whether or not the peer is a candidate to consider
// syncing from.
func (b *blockManager) isSyncCandidate(p *Peer) bool {
	// Typically a peer is not a candidate for sync if it's not a full node,
	// however regression test is special in that the regression tool is
	// not a full node and still needs to be considered a sync candidate.
	if b.node.cfg.RegressionTest {
		// The peer is not a candidate if it's not coming from localhost
		// or the hostname can't be determined for some reason.
		host, _, err := net.SplitHostPort(p.addr)
		if err != nil {
			return false
		}

		if host != "127.0.0.1" && host != "localhost" {
			return false
		}
	} else {
		// The peer is not a candidate for sync if it's not a full node.
		if p.services&btcwire.SFNodeNetwork != btcwire.SFNodeNetwork {
			return false
		}
	}

	// Candidate if all checks passed.
	return true
}

// handleNewPeerMsg deals with new peers that have signalled they may
// be considered as a sync peer (they have already successfully negotiated).  It
// also starts syncing if needed.  It is invoked from the syncHandler goroutine.
func (b *blockManager) handleNewPeerMsg(peers *list.List, p *Peer) {
	// Ignore if in the process of shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	log.Infof("New valid peer %s (%s)", p, p.userAgent)

	// Ignore the peer if it's not a sync candidate.
	if !b.isSyncCandidate(p) {
		return
	}

	// Add the peer as a candidate to sync from.
	peers.PushBack(p)

	// Start syncing by choosing the best candidate if needed.
	b.startSync(peers)
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was
// the current sync peer, attempts to select a new best peer to sync from.  It
// is invoked from the syncHandler goroutine.
func (b *blockManager) handleDonePeerMsg(peers *list.List, p *Peer) {
	// Remove the peer from the list of candidate peers.
	for e := peers.Front(); e != nil; e = e.Next() {
		if e.Value == p {
			peers.Remove(e)
			break
		}
	}

	log.Infof("Lost peer %s", p)

	// Remove requested transactions from the global map so that they will
	// be fetched from elsewhere next time we get an inv.
	for k := range p.requestedTxns {
		delete(b.requestedTxns, k)
	}

	// Remove requested blocks from the global map so that they will be
	// fetched from elsewhere next time we get an inv.
	// TODO(oga) we could possibly here check which peers have these blocks
	// and request them now to speed things up a little.
	for k := range p.requestedBlocks {
		delete(b.requestedBlocks, k)
	}

	// Attempt to find a new peer to sync from if the quitting peer is the
	// sync peer.  Also, reset the headers-first state if in headers-first
	// mode so
	if b.syncPeer != nil && b.syncPeer == p {
		b.syncPeer = nil
		if b.headersFirstMode {
			// This really shouldn't fail.  We have a fairly
			// unrecoverable database issue if it does.
			newestHash, height, err := b.node.db.NewestSha()
			if err != nil {
				log.Warnf("Unable to obtain latest "+
					"block information from the database: "+
					"%v", err)
				return
			}
			b.resetHeaderState(newestHash, height)
		}
		b.startSync(peers)
	}
}

// logBlockHeight logs a new block height as an information message to show
// progress to the user.  In order to prevent spam, it limits logging to one
// message every 10 seconds with duration and totals included.
func (b *blockManager) logBlockHeight(block *btcutil.Block) {
	b.receivedLogBlocks++
	b.receivedLogTx += int64(len(block.MsgBlock().Transactions))

	now := time.Now()
	duration := now.Sub(b.lastBlockLogTime)
	if duration < time.Second*10 {
		return
	}

	// Truncate the duration to 10s of milliseconds.
	durationMillis := int64(duration / time.Millisecond)
	tDuration := 10 * time.Millisecond * time.Duration(durationMillis/10)

	// Log information about new block height.
	blockStr := "blocks"
	if b.receivedLogBlocks == 1 {
		blockStr = "block"
	}
	txStr := "transactions"
	if b.receivedLogTx == 1 {
		txStr = "transaction"
	}
	log.Infof("Processed %d %s in the last %s (%d %s, height %d, %s)",
		b.receivedLogBlocks, blockStr, tDuration, b.receivedLogTx,
		txStr, block.Height(), block.MsgBlock().Header.Timestamp)

	b.receivedLogBlocks = 0
	b.receivedLogTx = 0
	b.lastBlockLogTime = now
}

// handleTxMsg handles transaction messages from all peers.
func (b *blockManager) handleTxMsg(tmsg *txMsg) {
	// NOTE:  BitcoinJ, and possibly other wallets, don't follow the spec of
	// sending an inventory message and allowing the remote peer to decide
	// whether or not they want to request the transaction via a getdata
	// message.  Unfortuantely the reference implementation permits
	// unrequested data, so it has allowed wallets that don't follow the
	// spec to proliferate.  While this is not ideal, there is no check here
	// to disconnect peers for sending unsolicited transactions to provide
	// interoperability.

	// Process the transaction to include validation, insertion in the
	// memory pool, orphan handling, etc.
	err := tmsg.peer.node.TxMemPool.ProcessTransaction(tmsg.tx, true, true)

	// Remove transaction from request maps. Either the mempool/chain
	// already knows about it and as such we shouldn't have any more
	// instances of trying to fetch it, or we failed to insert and thus
	// we'll retry next time we get an inv.
	txHash := tmsg.tx.Sha()
	delete(tmsg.peer.requestedTxns, *txHash)
	delete(b.requestedTxns, *txHash)

	if err != nil {
		// When the error is a rule error, it means the transaction was
		// simply rejected as opposed to something actually going wrong,
		// so log it as such.  Otherwise, something really did go wrong,
		// so log it as an actual error.
		if _, ok := err.(RuleError); ok {
			log.Debugf("Rejected transaction %v from %s: %v",
				txHash, tmsg.peer, err)
		} else {
			log.Errorf("Failed to process transaction %v: %v",
				txHash, err)
		}

		// Convert the error into an appropriate reject message and
		// send it.
		code, reason := errToRejectErr(err)
		tmsg.peer.xPushRejectMsg(btcwire.CmdBlock, code, reason, txHash,
			false)
		return
	}
}

// current returns true if we believe we are synced with our peers, false if we
// still have blocks to check
func (b *blockManager) current() bool {
	if !b.blockChain.IsCurrent(b.node.TimeSource) {
		return false
	}

	// if blockChain thinks we are current and we have no syncPeer it
	// is probably right.
	if b.syncPeer == nil {
		return true
	}

	_, height, err := b.node.db.NewestSha()
	// No matter what chain thinks, if we are below the block we are
	// syncing to we are not current.
	// TODO(oga) we can get chain to return the height of each block when we
	// parse an orphan, which would allow us to update the height of peers
	// from what it was at initial handshake.
	if err != nil || height < int64(b.syncPeer.lastBlock) {
		return false
	}
	return true
}

// handleBlockMsg handles block messages from all peers.
func (b *blockManager) handleBlockMsg(bmsg *blockMsg) {
	// If we didn't ask for this block then the peer is misbehaving.
	blockSha, _ := bmsg.block.Sha()
	if _, ok := bmsg.peer.requestedBlocks[*blockSha]; !ok {
		// The regression test intentionally sends some blocks twice
		// to test duplicate block insertion fails.  Don't disconnect
		// the peer or ignore the block when we're in regression test
		// mode in this case so the chain code is actually fed the
		// duplicate blocks.
		if !b.node.cfg.RegressionTest {
			log.Warnf("Got unrequested block %v from %s -- "+
				"disconnecting", blockSha, bmsg.peer.addr)
			bmsg.peer.xDisconnect()
			return
		}
	}

	// When in headers-first mode, if the block matches the hash of the
	// first header in the list of headers that are being fetched, it's
	// eligible for less validation since the headers have already been
	// verified to link together and are valid up to the next checkpoint.
	// Also, remove the list entry for all blocks except the checkpoint
	// since it is needed to verify the next round of headers links
	// properly.
	isCheckpointBlock := false
	behaviorFlags := btcchain.BFNone
	if b.headersFirstMode {
		firstNodeEl := b.headerList.Front()
		if firstNodeEl != nil {
			firstNode := firstNodeEl.Value.(*headerNode)
			if blockSha.IsEqual(firstNode.sha) {
				behaviorFlags |= btcchain.BFFastAdd
				if firstNode.sha.IsEqual(b.nextCheckpoint.Hash) {
					isCheckpointBlock = true
				} else {
					b.headerList.Remove(firstNodeEl)
				}
			}
		}
	}

	// Remove block from request maps. Either chain will know about it and
	// so we shouldn't have any more instances of trying to fetch it, or we
	// will fail the insert and thus we'll retry next time we get an inv.
	delete(bmsg.peer.requestedBlocks, *blockSha)
	delete(b.requestedBlocks, *blockSha)

	// Process the block to include validation, best chain selection, orphan
	// handling, etc.
	isOrphan, err := b.blockChain.ProcessBlock(bmsg.block,
		b.node.TimeSource, behaviorFlags)
	if err != nil {
		// When the error is a rule error, it means the block was simply
		// rejected as opposed to something actually going wrong, so log
		// it as such.  Otherwise, something really did go wrong, so log
		// it as an actual error.
		if _, ok := err.(btcchain.RuleError); ok {
			log.Infof("Rejected block %v from %s: %v", blockSha,
				bmsg.peer, err)
		} else {
			log.Errorf("Failed to process block %v: %v",
				blockSha, err)
		}

		// Convert the error into an appropriate reject message and
		// send it.
		code, reason := errToRejectErr(err)
		bmsg.peer.xPushRejectMsg(btcwire.CmdBlock, code, reason,
			blockSha, false)
		return
	}

	// Request the parents for the orphan block from the peer that sent it.
	if isOrphan {
		orphanRoot := b.blockChain.GetOrphanRoot(blockSha)
		locator, err := b.blockChain.LatestBlockLocator()
		if err != nil {
			log.Warnf("Failed to get block locator for the "+
				"latest block: %v", err)
		} else {
			bmsg.peer.xPushGetBlocksMsg(locator, orphanRoot)
		}
	} else {
		// When the block is not an orphan, log information about it and
		// update the chain state.

		b.logBlockHeight(bmsg.block)

		// Query the db for the latest best block since the block
		// that was processed could be on a side chain or have caused
		// a reorg.
		newestSha, newestHeight, _ := b.node.db.NewestSha()
		b.updateChainState(newestSha, newestHeight)

		// Allow any clients performing long polling via the
		// getblocktemplate RPC to be notified when the new block causes
		// their old block template to become stale.
		/*rpcServer := b.node.rpcServer
		if rpcServer != nil {
			rpcServer.gbtWorkState.NotifyBlockConnected(blockSha)
		}*/
		if b.NotifyBlockConnectedGBT != nil {
			b.NotifyBlockConnectedGBT(blockSha)
		}
	}

	// Sync the db to disk.
	b.node.db.Sync()

	// Nothing more to do if we aren't in headers-first mode.
	if !b.headersFirstMode {
		return
	}

	// This is headers-first mode, so if the block is not a checkpoint
	// request more blocks using the header list when the request queue is
	// getting short.
	if !isCheckpointBlock {
		if b.startHeader != nil &&
			len(bmsg.peer.requestedBlocks) < minInFlightBlocks {
			b.fetchHeaderBlocks()
		}
		return
	}

	// This is headers-first mode and the block is a checkpoint.  When
	// there is a next checkpoint, get the next round of headers by asking
	// for headers starting from the block after this one up to the next
	// checkpoint.
	prevHeight := b.nextCheckpoint.Height
	prevHash := b.nextCheckpoint.Hash
	b.nextCheckpoint = b.findNextHeaderCheckpoint(prevHeight)
	if b.nextCheckpoint != nil {
		locator := btcchain.BlockLocator([]*btcwire.ShaHash{prevHash})
		err := bmsg.peer.xPushGetHeadersMsg(locator, b.nextCheckpoint.Hash)
		if err != nil {
			log.Warnf("Failed to send getheaders message to "+
				"peer %s: %v", bmsg.peer.addr, err)
			return
		}
		log.Infof("Downloading headers for blocks %d to %d from "+
			"peer %s", prevHeight+1, b.nextCheckpoint.Height,
			b.syncPeer.addr)
		return
	}

	// This is headers-first mode, the block is a checkpoint, and there are
	// no more checkpoints, so switch to normal mode by requesting blocks
	// from the block after this one up to the end of the chain (zero hash).
	b.headersFirstMode = false
	b.headerList.Init()
	log.Infof("Reached the final checkpoint -- switching to normal mode")
	locator := btcchain.BlockLocator([]*btcwire.ShaHash{blockSha})
	err = bmsg.peer.xPushGetBlocksMsg(locator, &zeroHash)
	if err != nil {
		log.Warnf("Failed to send getblocks message to peer %s: %v",
			bmsg.peer.addr, err)
		return
	}
}

// fetchHeaderBlocks creates and sends a request to the syncPeer for the next
// list of blocks to be downloaded based on the current list of headers.
func (b *blockManager) fetchHeaderBlocks() {
	// Nothing to do if there is no start header.
	if b.startHeader == nil {
		log.Warnf("fetchHeaderBlocks called with no start header")
		return
	}

	// Build up a getdata request for the list of blocks the headers
	// describe.  The size hint will be limited to btcwire.MaxInvPerMsg by
	// the function, so no need to double check it here.
	gdmsg := btcwire.NewMsgGetDataSizeHint(uint(b.headerList.Len()))
	numRequested := 0
	for e := b.startHeader; e != nil; e = e.Next() {
		node, ok := e.Value.(*headerNode)
		if !ok {
			log.Warnf("Header list node type is not a headerNode")
			continue
		}

		iv := btcwire.NewInvVect(btcwire.InvTypeBlock, node.sha)
		haveInv, err := b.haveInventory(iv)
		if err != nil {
			log.Warnf("Unexpected failure when checking for "+
				"existing inventory during header block "+
				"fetch: %v", err)
		}
		if !haveInv {
			b.requestedBlocks[*node.sha] = struct{}{}
			b.syncPeer.requestedBlocks[*node.sha] = struct{}{}
			gdmsg.AddInvVect(iv)
			numRequested++
		}
		b.startHeader = e.Next()
		if numRequested >= btcwire.MaxInvPerMsg {
			break
		}
	}
	if len(gdmsg.InvList) > 0 {
		b.syncPeer.xQueueMessage(gdmsg, nil)
	}
}

// handleHeadersMsghandles headers messages from all peers.
func (b *blockManager) handleHeadersMsg(hmsg *headersMsg) {
	// The remote peer is misbehaving if we didn't request headers.
	msg := hmsg.headers
	numHeaders := len(msg.Headers)
	if !b.headersFirstMode {
		log.Warnf("Got %d unrequested headers from %s -- "+
			"disconnecting", numHeaders, hmsg.peer.addr)
		hmsg.peer.xDisconnect()
		return
	}

	// Nothing to do for an empty headers message.
	if numHeaders == 0 {
		return
	}

	// Process all of the received headers ensuring each one connects to the
	// previous and that checkpoints match.
	receivedCheckpoint := false
	var finalHash *btcwire.ShaHash
	for _, blockHeader := range msg.Headers {
		blockHash, err := blockHeader.BlockSha()
		if err != nil {
			log.Warnf("Failed to compute hash of header "+
				"received from peer %s -- disconnecting",
				hmsg.peer.addr)
			hmsg.peer.xDisconnect()
			return
		}
		finalHash = &blockHash

		// Ensure there is a previous header to compare against.
		prevNodeEl := b.headerList.Back()
		if prevNodeEl == nil {
			log.Warnf("Header list does not contain a previous" +
				"element as expected -- disconnecting peer")
			hmsg.peer.xDisconnect()
			return
		}

		// Ensure the header properly connects to the previous one and
		// add it to the list of headers.
		node := headerNode{sha: &blockHash}
		prevNode := prevNodeEl.Value.(*headerNode)
		if prevNode.sha.IsEqual(&blockHeader.PrevBlock) {
			node.height = prevNode.height + 1
			e := b.headerList.PushBack(&node)
			if b.startHeader == nil {
				b.startHeader = e
			}
		} else {
			log.Warnf("Received block header that does not"+
				"properly connect to the chain from peer %s "+
				"-- disconnecting", hmsg.peer.addr)
			hmsg.peer.xDisconnect()
			return
		}

		// Verify the header at the next checkpoint height matches.
		if node.height == b.nextCheckpoint.Height {
			if node.sha.IsEqual(b.nextCheckpoint.Hash) {
				receivedCheckpoint = true
				log.Infof("Verified downloaded block "+
					"header against checkpoint at height "+
					"%d/hash %s", node.height, node.sha)
			} else {
				log.Warnf("Block header at height %d/hash "+
					"%s from peer %s does NOT match "+
					"expected checkpoint hash of %s -- "+
					"disconnecting", node.height,
					node.sha, hmsg.peer.addr,
					b.nextCheckpoint.Hash)
				hmsg.peer.xDisconnect()
				return
			}
			break
		}
	}

	// When this header is a checkpoint, switch to fetching the blocks for
	// all of the headers since the last checkpoint.
	if receivedCheckpoint {
		// Since the first entry of the list is always the final block
		// that is already in the database and is only used to ensure
		// the next header links properly, it must be removed before
		// fetching the blocks.
		b.headerList.Remove(b.headerList.Front())
		log.Infof("Received %v block headers: Fetching blocks",
			b.headerList.Len())
		b.lastBlockLogTime = time.Now()
		b.fetchHeaderBlocks()
		return
	}

	// This header is not a checkpoint, so request the next batch of
	// headers starting from the latest known header and ending with the
	// next checkpoint.
	locator := btcchain.BlockLocator([]*btcwire.ShaHash{finalHash})
	err := hmsg.peer.xPushGetHeadersMsg(locator, b.nextCheckpoint.Hash)
	if err != nil {
		log.Warnf("Failed to send getheaders message to "+
			"peer %s: %v", hmsg.peer.addr, err)
		return
	}
}

// haveInventory returns whether or not the inventory represented by the passed
// inventory vector is known.  This includes checking all of the various places
// inventory can be when it is in different states such as blocks that are part
// of the main chain, on a side chain, in the orphan pool, and transactions that
// are in the memory pool (either the main pool or orphan pool).
func (b *blockManager) haveInventory(invVect *btcwire.InvVect) (bool, error) {
	switch invVect.Type {
	case btcwire.InvTypeBlock:
		// Ask chain if the block is known to it in any form (main
		// chain, side chain, or orphan).
		return b.blockChain.HaveBlock(&invVect.Hash)

	case btcwire.InvTypeTx:
		// Ask the transaction memory pool if the transaction is known
		// to it in any form (main pool or orphan).
		if b.node.TxMemPool.HaveTransaction(&invVect.Hash) {
			return true, nil
		}

		// Check if the transaction exists from the point of view of the
		// end of the main chain.
		return b.node.db.ExistsTxSha(&invVect.Hash)
	}

	// The requested inventory is is an unsupported type, so just claim
	// it is known to avoid requesting it.
	return true, nil
}

// handleInvMsg handles inv messages from all peers.
// We examine the inventory advertised by the remote peer and act accordingly.
func (b *blockManager) handleInvMsg(imsg *invMsg) {
	// Ignore invs from peers that aren't the sync if we are not current.
	// Helps prevent fetching a mass of orphans.
	if imsg.peer != b.syncPeer && !b.current() {
		return
	}

	// Attempt to find the final block in the inventory list.  There may
	// not be one.
	lastBlock := -1
	invVects := imsg.inv.InvList
	for i := len(invVects) - 1; i >= 0; i-- {
		if invVects[i].Type == btcwire.InvTypeBlock {
			lastBlock = i
			break
		}
	}

	// Request the advertised inventory if we don't already have it.  Also,
	// request parent blocks of orphans if we receive one we already have.
	// Finally, attempt to detect potential stalls due to long side chains
	// we already have and request more blocks to prevent them.
	chain := b.blockChain
	for i, iv := range invVects {
		// Ignore unsupported inventory types.
		if iv.Type != btcwire.InvTypeBlock && iv.Type != btcwire.InvTypeTx {
			continue
		}

		// Add the inventory to the cache of known inventory
		// for the peer.
		imsg.peer.xAddKnownInventory(iv)

		// Ignore inventory when we're in headers-first mode.
		if b.headersFirstMode {
			continue
		}

		// Request the inventory if we don't already have it.
		haveInv, err := b.haveInventory(iv)
		if err != nil {
			log.Warnf("Unexpected failure when checking for "+
				"existing inventory during inv message "+
				"processing: %v", err)
			continue
		}
		if !haveInv {
			// Add it to the request queue.
			imsg.peer.requestQueue.PushBack(iv)
			continue
		}

		if iv.Type == btcwire.InvTypeBlock {
			// The block is an orphan block that we already have.
			// When the existing orphan was processed, it requested
			// the missing parent blocks.  When this scenario
			// happens, it means there were more blocks missing
			// than are allowed into a single inventory message.  As
			// a result, once this peer requested the final
			// advertised block, the remote peer noticed and is now
			// resending the orphan block as an available block
			// to signal there are more missing blocks that need to
			// be requested.
			if chain.IsKnownOrphan(&iv.Hash) {
				// Request blocks starting at the latest known
				// up to the root of the orphan that just came
				// in.
				orphanRoot := chain.GetOrphanRoot(&iv.Hash)
				locator, err := chain.LatestBlockLocator()
				if err != nil {
					log.Errorf("PEER: Failed to get block "+
						"locator for the latest block: "+
						"%v", err)
					continue
				}
				imsg.peer.xPushGetBlocksMsg(locator, orphanRoot)
				continue
			}

			// We already have the final block advertised by this
			// inventory message, so force a request for more.  This
			// should only happen if we're on a really long side
			// chain.
			if i == lastBlock {
				// Request blocks after this one up to the
				// final one the remote peer knows about (zero
				// stop hash).
				locator := chain.BlockLocatorFromHash(&iv.Hash)
				imsg.peer.xPushGetBlocksMsg(locator, &zeroHash)
			}
		}
	}

	// Request as much as possible at once.  Anything that won't fit into
	// the request will be requested on the next inv message.
	numRequested := 0
	gdmsg := btcwire.NewMsgGetData()
	requestQueue := imsg.peer.requestQueue
	for e := requestQueue.Front(); e != nil; e = requestQueue.Front() {
		iv := e.Value.(*btcwire.InvVect)
		imsg.peer.requestQueue.Remove(e)

		switch iv.Type {
		case btcwire.InvTypeBlock:
			// Request the block if there is not already a pending
			// request.
			if _, exists := b.requestedBlocks[iv.Hash]; !exists {
				b.requestedBlocks[iv.Hash] = struct{}{}
				imsg.peer.requestedBlocks[iv.Hash] = struct{}{}
				gdmsg.AddInvVect(iv)
				numRequested++
			}

		case btcwire.InvTypeTx:
			// Request the transaction if there is not already a
			// pending request.
			if _, exists := b.requestedTxns[iv.Hash]; !exists {
				b.requestedTxns[iv.Hash] = struct{}{}
				imsg.peer.requestedTxns[iv.Hash] = struct{}{}
				gdmsg.AddInvVect(iv)
				numRequested++
			}
		}

		if numRequested >= btcwire.MaxInvPerMsg {
			break
		}
	}
	if len(gdmsg.InvList) > 0 {
		imsg.peer.xQueueMessage(gdmsg, nil)
	}
}

// blockHandler is the main handler for the block manager.  It must be run
// as a goroutine.  It processes block and inv messages in a separate goroutine
// from the peer handlers so the block (MsgBlock) messages are handled by a
// single thread without needing to lock memory data structures.  This is
// important because the block manager controls which blocks are needed and how
// the fetching should proceed.
func (b *blockManager) blockHandler() {
	candidatePeers := list.New()
out:
	for {
		select {
		case m := <-b.msgChan:
			switch msg := m.(type) {
			case *newPeerMsg:
				b.handleNewPeerMsg(candidatePeers, msg.peer)

			case *txMsg:
				b.handleTxMsg(msg)
				msg.peer.txProcessed <- struct{}{}

			case *blockMsg:
				b.handleBlockMsg(msg)
				msg.peer.blockProcessed <- struct{}{}

			case *invMsg:
				b.handleInvMsg(msg)

			case *headersMsg:
				b.handleHeadersMsg(msg)

			case *donePeerMsg:
				b.handleDonePeerMsg(candidatePeers, msg.peer)

			case getSyncPeerMsg:
				msg.reply <- b.syncPeer

			case checkConnectBlockMsg:
				err := b.blockChain.CheckConnectBlock(msg.block)
				msg.reply <- err

			case calcNextReqDifficultyMsg:
				difficulty, err :=
					b.blockChain.CalcNextRequiredDifficulty(
						msg.timestamp)
				msg.reply <- calcNextReqDifficultyResponse{
					difficulty: difficulty,
					err:        err,
				}

			case processBlockMsg:
				isOrphan, err := b.blockChain.ProcessBlock(
					msg.block, b.node.TimeSource,
					msg.flags)
				if err != nil {
					msg.reply <- processBlockResponse{
						isOrphan: false,
						err:      err,
					}
				}

				// Query the db for the latest best block since
				// the block that was processed could be on a
				// side chain or have caused a reorg.
				newestSha, newestHeight, _ := b.node.db.NewestSha()
				b.updateChainState(newestSha, newestHeight)

				msg.reply <- processBlockResponse{
					isOrphan: isOrphan,
					err:      nil,
				}

			case isCurrentMsg:
				msg.reply <- b.current()

			default:
				log.Warnf("Invalid message type in block "+
					"handler: %T", msg)
			}

		case <-b.quit:
			break out
		}
	}

	b.wg.Done()
	log.Tracef("Block handler done")
}

// handleNotifyMsg handles notifications from btcchain.  It does things such
// as request orphan block parents and relay accepted blocks to connected peers.
func (b *blockManager) handleNotifyMsg(notification *btcchain.Notification) {
	switch notification.Type {
	// A block has been accepted into the block chain.  Relay it to other
	// peers.
	case btcchain.NTBlockAccepted:
		// Don't relay if we are not current. Other peers that are
		// current should already know about it.

		if !b.current() {
			return
		}

		block, ok := notification.Data.(*btcutil.Block)
		if !ok {
			log.Warnf("Chain accepted notification is not a block.")
			break
		}

		// It's ok to ignore the error here since the notification is
		// coming from the chain code which has already cached the hash.
		hash, _ := block.Sha()

		// Generate the inventory vector and relay it.
		iv := btcwire.NewInvVect(btcwire.InvTypeBlock, hash)
		b.node.RelayInventory(iv)

	// A block has been connected to the main block chain.
	case btcchain.NTBlockConnected:
		block, ok := notification.Data.(*btcutil.Block)
		if !ok {
			log.Warnf("Chain connected notification is not a block.")
			break
		}

		// Remove all of the transactions (except the coinbase) in the
		// connected block from the transaction pool.  Secondly, remove any
		// transactions which are now double spends as a result of these
		// new transactions.  Finally, remove any transaction that is
		// no longer an orphan.  Note that removing a transaction from
		// pool also removes any transactions which depend on it,
		// recursively.
		for _, tx := range block.Transactions()[1:] {
			b.node.TxMemPool.xRemoveTransaction(tx)
			b.node.TxMemPool.xRemoveDoubleSpends(tx)
			b.node.TxMemPool.xRemoveOrphan(tx.Sha())
		}

		if b.NotifyBlockConnected != nil {
			for _, tx := range block.Transactions()[1:] {
				iv := btcwire.NewInvVect(btcwire.InvTypeTx, tx.Sha())
				b.node.RemoveRebroadcastInventory(iv)
			}

			b.NotifyBlockConnected(block)
		}

		/*		if r := b.node.rpcServer; r != nil {
				// Now that this block is in the blockchain we can mark
				// all the transactions (except the coinbase) as no
				// longer needing rebroadcasting.
				for _, tx := range block.Transactions()[1:] {
					iv := btcwire.NewInvVect(btcwire.InvTypeTx, tx.Sha())
					b.node.RemoveRebroadcastInventory(iv)
				}

				// Notify registered websocket clients of incoming block.
				r.ntfnMgr.NotifyBlockConnected(block)
			}*/

	// A block has been disconnected from the main block chain.
	case btcchain.NTBlockDisconnected:
		block, ok := notification.Data.(*btcutil.Block)
		if !ok {
			log.Warnf("Chain disconnected notification is not a block.")
			break
		}

		// Reinsert all of the transactions (except the coinbase) into
		// the transaction pool.
		for _, tx := range block.Transactions()[1:] {
			err := b.node.TxMemPool.xMaybeAcceptTransaction(tx, nil, false, true)
			if err != nil {
				// Remove the transaction and all transactions
				// that depend on it if it wasn't accepted into
				// the transaction pool.
				b.node.TxMemPool.xRemoveTransaction(tx)
			}
		}

		// Notify registered websocket clients.
		if b.NotifyBlockDisconnected != nil {
			b.NotifyBlockDisconnected(block)
		}

		/*if r := b.node.rpcServer; r != nil {
			r.ntfnMgr.NotifyBlockDisconnected(block)
		}*/
	}
}

// NewPeer informs the block manager of a newly active peer.
func (b *blockManager) NewPeer(p *Peer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &newPeerMsg{peer: p}
}

// QueueTx adds the passed transaction message and peer to the block handling
// queue.
func (b *blockManager) QueueTx(tx *btcutil.Tx, p *Peer) {
	// Don't accept more transactions if we're shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		p.txProcessed <- struct{}{}
		return
	}

	b.msgChan <- &txMsg{tx: tx, peer: p}
}

// QueueBlock adds the passed block message and peer to the block handling queue.
func (b *blockManager) QueueBlock(block *btcutil.Block, p *Peer) {
	// Don't accept more blocks if we're shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		p.blockProcessed <- struct{}{}
		return
	}

	b.msgChan <- &blockMsg{block: block, peer: p}
}

// QueueInv adds the passed inv message and peer to the block handling queue.
func (b *blockManager) QueueInv(inv *btcwire.MsgInv, p *Peer) {
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &invMsg{inv: inv, peer: p}
}

// QueueHeaders adds the passed headers message and peer to the block handling
// queue.
func (b *blockManager) QueueHeaders(headers *btcwire.MsgHeaders, p *Peer) {
	// No channel handling here because peers do not need to block on
	// headers messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &headersMsg{headers: headers, peer: p}
}

// DonePeer informs the blockmanager that a peer has disconnected.
func (b *blockManager) DonePeer(p *Peer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &donePeerMsg{peer: p}
}

// Start begins the core block handler which processes block and inv messages.
func (b *blockManager) Start() {
	// Already started?
	if atomic.AddInt32(&b.started, 1) != 1 {
		return
	}

	log.Tracef("Starting block manager")
	b.wg.Add(1)
	go b.blockHandler()
}

// Stop gracefully shuts down the block manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (b *blockManager) Stop() error {
	if atomic.AddInt32(&b.shutdown, 1) != 1 {
		log.Warnf("Block manager is already in the process of " +
			"shutting down")
		return nil
	}

	log.Infof("Block manager shutting down")
	close(b.quit)
	b.wg.Wait()
	return nil
}

// SyncPeer returns the current sync peer.
func (b *blockManager) SyncPeer() *Peer {
	reply := make(chan *Peer)
	b.msgChan <- getSyncPeerMsg{reply: reply}
	return <-reply
}

// CheckConnectBlock performs several checks to confirm connecting the passed
// block to the main chain does not violate any rules.  This function makes use
// of CheckConnectBlock on an internal instance of a block chain.  It is funneled
// through the block manager since btcchain is not safe for concurrent access.
func (b *blockManager) CheckConnectBlock(block *btcutil.Block) error {
	reply := make(chan error)
	b.msgChan <- checkConnectBlockMsg{block: block, reply: reply}
	return <-reply
}

// CalcNextRequiredDifficulty calculates the required difficulty for the next
// block after the current main chain.  This function makes use of
// CalcNextRequiredDifficulty on an internal instance of a block chain.  It is
// funneled through the block manager since btcchain is not safe for concurrent
// access.
func (b *blockManager) CalcNextRequiredDifficulty(timestamp time.Time) (uint32, error) {
	reply := make(chan calcNextReqDifficultyResponse)
	b.msgChan <- calcNextReqDifficultyMsg{timestamp: timestamp, reply: reply}
	response := <-reply
	return response.difficulty, response.err
}

// ProcessBlock makes use of ProcessBlock on an internal instance of a block
// chain.  It is funneled through the block manager since btcchain is not safe
// for concurrent access.
func (b *blockManager) ProcessBlock(block *btcutil.Block, flags btcchain.BehaviorFlags) (bool, error) {
	reply := make(chan processBlockResponse, 1)
	b.msgChan <- processBlockMsg{block: block, flags: flags, reply: reply}
	response := <-reply
	return response.isOrphan, response.err
}

// IsCurrent returns whether or not the block manager believes it is synced with
// the connected peers.
func (b *blockManager) IsCurrent() bool {
	reply := make(chan bool)
	b.msgChan <- isCurrentMsg{reply: reply}
	return <-reply
}

// newBlockManager returns a new bitcoin block manager.
// Use Start to begin processing asynchronous block and inv updates.
func newBlockManager(s *Node) (*blockManager, error) {
	newestHash, height, err := s.db.NewestSha()
	if err != nil {
		return nil, err
	}

	bm := blockManager{
		node:             s,
		requestedTxns:    make(map[btcwire.ShaHash]struct{}),
		requestedBlocks:  make(map[btcwire.ShaHash]struct{}),
		lastBlockLogTime: time.Now(),
		msgChan:          make(chan interface{}, s.cfg.MaxPeers*3),
		headerList:       list.New(),
		quit:             make(chan struct{}),
	}
	bm.blockChain = btcchain.New(s.db, s.cfg.ActiveNetParams, bm.handleNotifyMsg)
	bm.blockChain.DisableCheckpoints(s.cfg.DisableCheckpoints)
	if !s.cfg.DisableCheckpoints {
		// Initialize the next checkpoint based on the current height.
		bm.nextCheckpoint = bm.findNextHeaderCheckpoint(height)
		if bm.nextCheckpoint != nil {
			bm.resetHeaderState(newestHash, height)
		}
	} else {
		log.Infof("Checkpoints are disabled")
	}

	log.Infof("Generating initial block node index.  This may " +
		"take a while...")
	err = bm.blockChain.GenerateInitialIndex()
	if err != nil {
		return nil, err
	}
	log.Infof("Block index generation complete")

	// Initialize the chain state now that the intial block node index has
	// been generated.
	bm.updateChainState(newestHash, height)

	return &bm, nil
}

// removeRegressionDB removes the existing regression test database if running
// in regression test mode and it already exists.
func (cfg *NodeConfig) removeRegressionDB(dbPath string) error {
	// Dont do anything if not in regression test mode.
	if !cfg.RegressionTest {
		return nil
	}

	// Remove the old regression test database if it already exists.
	fi, err := os.Stat(dbPath)
	if err == nil {
		log.Infof("Removing regression test database from '%s'", dbPath)
		if fi.IsDir() {
			err := os.RemoveAll(dbPath)
			if err != nil {
				return err
			}
		} else {
			err := os.Remove(dbPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// dbPath returns the path to the block database given a database type.
func (cfg *NodeConfig) blockDbPath(dbType string) string {
	// The database name is based on the database type.
	dbName := blockDbNamePrefix + "_" + dbType
	if dbType == "sqlite" {
		dbName = dbName + ".db"
	}
	dbPath := filepath.Join(cfg.DataDir, dbName)
	return dbPath
}

func (cfg *NodeConfig) nameDbPath(dbType string) string {
	dbName := nameDbNamePrefix + "_" + dbType
	if dbType == "sqlite" {
		dbName = dbName + ".db"
	}
	dbPath := filepath.Join(cfg.DataDir, dbName)
	return dbPath
}

// warnMultipeDBs shows a warning if multiple block database types are detected.
// This is not a situation most users want.  It is handy for development however
// to support multiple side-by-side databases.
func (cfg *NodeConfig) warnMultipleDBs() {
	// This is intentionally not using the known db types which depend
	// on the database types compiled into the binary since we want to
	// detect legacy db types as well.
	dbTypes := []string{"leveldb", "sqlite"}
	duplicateDbPaths := make([]string, 0, len(dbTypes)-1)
	for _, dbType := range dbTypes {
		if dbType == cfg.DbType {
			continue
		}

		// Store db path as a duplicate db if it exists.
		dbPath := cfg.blockDbPath(dbType)
		if fileExists(dbPath) {
			duplicateDbPaths = append(duplicateDbPaths, dbPath)
		}
	}

	// Warn if there are extra databases.
	if len(duplicateDbPaths) > 0 {
		selectedDbPath := cfg.blockDbPath(cfg.DbType)
		log.Warnf("WARNING: There are multiple block chain databases "+
			"using different database types.\nYou probably don't "+
			"want to waste disk space by having more than one.\n"+
			"Your current database is located at [%v].\nThe "+
			"additional database is located at %v", selectedDbPath,
			duplicateDbPaths)
	}
}

// setupBlockDB loads (or creates when needed) the block database taking into
// account the selected database backend.  It also contains additional logic
// such warning the user if there are multiple databases which consume space on
// the file system and ensuring the regression test database is clean when in
// regression test mode.
func (cfg *NodeConfig) setupBlockDB() (btcdb.Db, error) {
	// The memdb backend does not have a file path associated with it, so
	// handle it uniquely.  We also don't want to worry about the multiple
	// database type warnings when running with the memory database.
	if cfg.DbType == "memdb" {
		log.Infof("Creating block database in memory.")
		db, err := btcdb.CreateDB(cfg.DbType)
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	cfg.warnMultipleDBs()

	// The database name is based on the database type.
	dbPath := cfg.blockDbPath(cfg.DbType)

	// The regression test is special in that it needs a clean database for
	// each run, so remove it now if it already exists.
	cfg.removeRegressionDB(dbPath)

	log.Infof("Loading block database from '%s'", dbPath)
	db, err := btcdb.OpenDB(cfg.DbType, dbPath)
	if err != nil {
		// Return the error if it's not because the database
		// doesn't exist.
		if err != btcdb.ErrDbDoesNotExist {
			return nil, err
		}

		// Create the db if it does not exist.
		err = os.MkdirAll(cfg.DataDir, 0700)
		if err != nil {
			return nil, err
		}
		db, err = btcdb.CreateDB(cfg.DbType, dbPath)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// loadBlockDB opens the block database and returns a handle to it.
func (cfg *NodeConfig) LoadBlockDB() (btcdb.Db, error) {
	db, err := cfg.setupBlockDB()
	if err != nil {
		return nil, err
	}

	// Get the latest block height from the database.
	_, height, err := db.NewestSha()
	if err != nil {
		db.Close()
		return nil, err
	}

	// Insert the appropriate genesis block for the bitcoin network being
	// connected to if needed.
	if height == -1 {
		genesis := btcutil.NewBlock(cfg.ActiveNetParams.GenesisBlock)
		_, err := db.InsertBlock(genesis)
		if err != nil {
			db.Close()
			return nil, err
		}
		log.Infof("Inserted genesis block %v",
			cfg.ActiveNetParams.GenesisHash)
		height = 0
	}

	log.Infof("Block database loaded with block height %d", height)
	return db, nil
}
