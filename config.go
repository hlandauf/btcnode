// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcnode

import (
	"net"
	"strings"
	"time"
  "os"

	"github.com/hlandauf/btcdb"
	_ "github.com/hlandauf/btcdb/ldb"
	_ "github.com/hlandauf/btcdb/memdb"
	"github.com/hlandauf/btcnet"
	"github.com/hlandauf/btcutil"
)

const defaultBlockPrioritySize = 50000

// Config for BlockManager:
//   DisableCheckpoints bool
//   RegressionTest bool
//   MaxPeers int
//   DataDir string
//   DbType string

// config defines the configuration options for btcd.
//
// See loadConfig for details on the configuration load process.
type NodeConfig struct {
	ActiveNetParams *btcnet.Params

	ShowVersion        bool          `short:"V" long:"version" description:"Display version information and exit"`
	ConfigFile         string        `short:"C" long:"configfile" description:"Path to configuration file"`
	DataDir            string        `short:"b" long:"datadir" description:"Directory to store data"`
	LogDir             string        `long:"logdir" description:"Directory to log output."`
	AddPeers           []string      `short:"a" long:"addpeer" description:"Add a peer to connect with at startup"`
	ConnectPeers       []string      `long:"connect" description:"Connect only to the specified peers at startup"`
	DisableListen      bool          `long:"nolisten" description:"Disable listening for incoming connections -- NOTE: Listening is automatically disabled if the --connect or --proxy options are used without also specifying listen interfaces via --listen"`
	Listeners          []string      `long:"listen" description:"Add an interface/port to listen for connections (default all interfaces port: 8333, testnet: 18333)"`
	MaxPeers           int           `long:"maxpeers" description:"Max number of inbound and outbound peers"`
	BanDuration        time.Duration `long:"banduration" description:"How long to ban misbehaving peers.  Valid time units are {s, m, h}.  Minimum 1 second"`
	DisableDNSSeed     bool          `long:"nodnsseed" description:"Disable DNS seeding for peers"`
	ExternalIPs        []string      `long:"externalip" description:"Add an ip to the list of local addresses we claim to listen on to peers"`
	Proxy              string        `long:"proxy" description:"Connect via SOCKS5 proxy (eg. 127.0.0.1:9050)"`
	ProxyUser          string        `long:"proxyuser" description:"Username for proxy server"`
	ProxyPass          string        `long:"proxypass" default-mask:"-" description:"Password for proxy server"`
	OnionProxy         string        `long:"onion" description:"Connect to tor hidden services via SOCKS5 proxy (eg. 127.0.0.1:9050)"`
	OnionProxyUser     string        `long:"onionuser" description:"Username for onion proxy server"`
	OnionProxyPass     string        `long:"onionpass" default-mask:"-" description:"Password for onion proxy server"`
	NoOnion            bool          `long:"noonion" description:"Disable connecting to tor hidden services"`
	TestNet3           bool          `long:"testnet" description:"Use the test network"`
	RegressionTest     bool          `long:"regtest" description:"Use the regression test network"`
	SimNet             bool          `long:"simnet" description:"Use the simulation test network"`
	DisableCheckpoints bool          `long:"nocheckpoints" description:"Disable built-in checkpoints.  Don't do this unless you know what you're doing."`
	DbType             string        `long:"dbtype" description:"Database backend to use for the Block Chain"`
	Profile            string        `long:"profile" description:"Enable HTTP profiling on given port -- NOTE port must be between 1024 and 65536"`
	CPUProfile         string        `long:"cpuprofile" description:"Write CPU profile to the specified file"`
	DebugLevel         string        `short:"d" long:"debuglevel" description:"Logging level for all subsystems {trace, debug, info, warn, error, critical} -- You may also specify <subsystem>=<level>,<subsystem2>=<level>,... to set the log level for individual subsystems -- Use show to list available subsystems"`
	Upnp               bool          `long:"upnp" description:"Use UPnP to map our listening port outside of NAT"`
	FreeTxRelayLimit   float64       `long:"limitfreerelay" description:"Limit relay of transactions with no transaction fee to the given amount in thousands of bytes per minute"`
	Generate           bool          `long:"generate" description:"Generate (mine) bitcoins using the CPU"`
	MiningAddrs        []string      `long:"miningaddr" description:"Add the specified payment address to the list of addresses to use for generated blocks -- At least one address is required if the generate option is set"`
	BlockMinSize       uint32        `long:"blockminsize" description:"Mininum block size in bytes to be used when creating a block"`
	BlockMaxSize       uint32        `long:"blockmaxsize" description:"Maximum block size in bytes to be used when creating a block"`
	BlockPrioritySize  uint32        `long:"blockprioritysize" description:"Size in bytes for high-priority/low-fee transactions when creating a block"`
	GetWorkKeys        []string      `long:"getworkkey" description:"DEPRECATED -- Use the --miningaddr option instead"`
	Onionlookup        func(string) ([]net.IP, error)
	Lookup             func(string) ([]net.IP, error)
	Oniondial          func(string, string) (net.Conn, error)
	Dial               func(string, string) (net.Conn, error)
	MiningAddrsS       []btcutil.Address

	DB               btcdb.Db
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// btcdDial connects to the address on the named network using the appropriate
// dial function depending on the address and configuration options.  For
// example, .onion addresses will be dialed using the onion specific proxy if
// one was specified, but will otherwise use the normal dial function (which
// could itself use a proxy or not).
func (n *Node) btcdDial(network, address string) (net.Conn, error) {
	if strings.HasSuffix(address, ".onion") {
		return n.cfg.Oniondial(network, address)
	}
	return n.cfg.Dial(network, address)
}

// btcdLookup returns the correct DNS lookup function to use depending on the
// passed host and configuration options.  For example, .onion addresses will be
// resolved using the onion specific proxy if one was specified, but will
// otherwise treat the normal proxy as tor unless --noonion was specified in
// which case the lookup will fail.  Meanwhile, normal IP addresses will be
// resolved using tor if a proxy was specified unless --noonion was also
// specified in which case the normal system DNS resolver will be used.
func (cfg *NodeConfig) btcdLookup(host string) ([]net.IP, error) {
	if strings.HasSuffix(host, ".onion") {
		return cfg.Onionlookup(host)
	}
	return cfg.Lookup(host)
}

func (n *Node) BtcdLookup(host string) ([]net.IP, error) {
	return n.cfg.btcdLookup(host)
}
