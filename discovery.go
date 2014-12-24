// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcnode

import "net"

// dnsDiscover looks up the list of peers resolved by DNS for all hosts in
// seeders. If proxy is not "" then it is used as a tor proxy for the
// resolution.
func (n *Node) dnsDiscover(seeder string) ([]net.IP, error) {
	peers, err := n.BtcdLookup(seeder)
	if err != nil {
		return nil, err
	}

	return peers, nil
}
