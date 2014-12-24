// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcnode

import (
	"fmt"
	"strings"
	"time"

	"github.com/hlandauf/btcwire"
  "github.com/hlandau/xlog"
)

var log, Log = xlog.New("btc.node")

const (
	// lockTimeThreshold is the number below which a lock time is
	// interpreted to be a block number.  Since an average of one block
	// is generated per 10 minutes, this allows blocks for about 9,512
	// years.  However, if the field is interpreted as a timestamp, given
	// the lock time is a uint32, the max is sometime around 2106.
	lockTimeThreshold uint32 = 5e8 // Tue Nov 5 00:53:20 1985 UTC

	// maxRejectReasonLen is the maximum length of a sanitized reject reason
	// that will be logged.
	maxRejectReasonLen = 200
)

// directionString is a helper function that returns a string that represents
// the direction of a connection (inbound or outbound).
func directionString(inbound bool) string {
	if inbound {
		return "inbound"
	}
	return "outbound"
}

// formatLockTime returns a transaction lock time as a human-readable string.
func formatLockTime(lockTime uint32) string {
	// The lock time field of a transaction is either a block height at
	// which the transaction is finalized or a timestamp depending on if the
	// value is before the lockTimeThreshold.  When it is under the
	// threshold it is a block height.
	if lockTime < lockTimeThreshold {
		return fmt.Sprintf("height %d", lockTime)
	}

	return time.Unix(int64(lockTime), 0).String()
}

// invSummary returns an inventory message as a human-readable string.
func invSummary(invList []*btcwire.InvVect) string {
	// No inventory.
	invLen := len(invList)
	if invLen == 0 {
		return "empty"
	}

	// One inventory item.
	if invLen == 1 {
		iv := invList[0]
		switch iv.Type {
		case btcwire.InvTypeError:
			return fmt.Sprintf("error %s", iv.Hash)
		case btcwire.InvTypeBlock:
			return fmt.Sprintf("block %s", iv.Hash)
		case btcwire.InvTypeTx:
			return fmt.Sprintf("tx %s", iv.Hash)
		}

		return fmt.Sprintf("unknown (%d) %s", uint32(iv.Type), iv.Hash)
	}

	// More than one inv item.
	return fmt.Sprintf("size %d", invLen)
}

// locatorSummary returns a block locator as a human-readable string.
func locatorSummary(locator []*btcwire.ShaHash, stopHash *btcwire.ShaHash) string {
	if len(locator) > 0 {
		return fmt.Sprintf("locator %s, stop %s", locator[0], stopHash)
	}

	return fmt.Sprintf("no locator, stop %s", stopHash)

}

// sanitizeString strips any characters which are even remotely dangerous, such
// as html control characters, from the passed string.  It also limits it to
// the passed maximum size, which can be 0 for unlimited.  When the string is
// limited, it will also add "..." to the string to indicate it was truncated.
func sanitizeString(str string, maxLength uint) string {
	const safeChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY" +
		"Z01234567890 .,;_/:?@"

	// Strip any characters not in the safeChars string removed.
	str = strings.Map(func(r rune) rune {
		if strings.IndexRune(safeChars, r) >= 0 {
			return r
		}
		return -1
	}, str)

	// Limit the string to the max allowed length.
	if maxLength > 0 && uint(len(str)) > maxLength {
		str = str[:maxLength]
		str = str + "..."
	}
	return str
}

// messageSummary returns a human-readable string which summarizes a message.
// Not all messages have or need a summary.  This is used for debug logging.
func messageSummary(msg btcwire.Message) string {
	switch msg := msg.(type) {
	case *btcwire.MsgVersion:
		return fmt.Sprintf("agent %s, pver %d, block %d",
			msg.UserAgent, msg.ProtocolVersion, msg.LastBlock)

	case *btcwire.MsgVerAck:
		// No summary.

	case *btcwire.MsgGetAddr:
		// No summary.

	case *btcwire.MsgAddr:
		return fmt.Sprintf("%d addr", len(msg.AddrList))

	case *btcwire.MsgPing:
		// No summary - perhaps add nonce.

	case *btcwire.MsgPong:
		// No summary - perhaps add nonce.

	case *btcwire.MsgAlert:
		// No summary.

	case *btcwire.MsgMemPool:
		// No summary.

	case *btcwire.MsgTx:
		hash, _ := msg.TxSha()
		return fmt.Sprintf("hash %s, %d inputs, %d outputs, lock %s",
			hash, len(msg.TxIn), len(msg.TxOut),
			formatLockTime(msg.LockTime))

	case *btcwire.MsgBlock:
		header := &msg.Header
		hash, _ := msg.BlockSha()
		return fmt.Sprintf("hash %s, ver %d, %d tx, %s", hash,
			header.Version, len(msg.Transactions), header.Timestamp)

	case *btcwire.MsgInv:
		return invSummary(msg.InvList)

	case *btcwire.MsgNotFound:
		return invSummary(msg.InvList)

	case *btcwire.MsgGetData:
		return invSummary(msg.InvList)

	case *btcwire.MsgGetBlocks:
		return locatorSummary(msg.BlockLocatorHashes, &msg.HashStop)

	case *btcwire.MsgGetHeaders:
		return locatorSummary(msg.BlockLocatorHashes, &msg.HashStop)

	case *btcwire.MsgHeaders:
		return fmt.Sprintf("num %d", len(msg.Headers))

	case *btcwire.MsgReject:
		// Ensure the variable length strings don't contain any
		// characters which are even remotely dangerous such as HTML
		// control characters, etc.  Also limit them to sane length for
		// logging.
		rejCommand := sanitizeString(msg.Cmd, btcwire.CommandSize)
		rejReason := sanitizeString(msg.Reason, maxRejectReasonLen)
		summary := fmt.Sprintf("cmd %v, code %v, reason %v", rejCommand,
			msg.Code, rejReason)
		if rejCommand == btcwire.CmdBlock || rejCommand == btcwire.CmdTx {
			summary += fmt.Sprintf(", hash %v", msg.Hash)
		}
		return summary
	}

	// No summary for other messages.
	return ""
}
