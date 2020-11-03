package mgo

import (
	"errors"
	"fmt"
	"github.com/agilecho/tec/mongo/mgo/bson"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mongoCluster struct {
	sync.RWMutex
	serverSynced sync.Cond
	userSeeds    []string
	dynaSeeds    []string
	servers      mongoServers
	masters      mongoServers
	references   int
	syncing      bool
	direct       bool
	failFast     bool
	syncCount    uint
	setName      string
	cachedIndex  map[string]bool
	sync         chan bool
	dial         dialer
}

func newCluster(userSeeds []string, direct, failFast bool, dial dialer, setName string) *mongoCluster {
	cluster := &mongoCluster{
		userSeeds:  userSeeds,
		references: 1,
		direct:     direct,
		failFast:   failFast,
		dial:       dial,
		setName:    setName,
	}
	cluster.serverSynced.L = cluster.RWMutex.RLocker()
	cluster.sync = make(chan bool, 1)
	stats.cluster(+1)
	go cluster.syncServersLoop()
	return cluster
}

func (cluster *mongoCluster) Acquire() {
	cluster.Lock()
	cluster.references++
	cluster.Unlock()
}

func (cluster *mongoCluster) Release() {
	cluster.Lock()
	if cluster.references == 0 {
		panic("cluster.Release() with references == 0")
	}
	cluster.references--
	if cluster.references == 0 {
		for _, server := range cluster.servers.Slice() {
			server.Close()
		}
		cluster.syncServers()
		stats.cluster(-1)
	}
	cluster.Unlock()
}

func (cluster *mongoCluster) LiveServers() (servers []string) {
	cluster.RLock()
	for _, serv := range cluster.servers.Slice() {
		servers = append(servers, serv.Addr)
	}
	cluster.RUnlock()
	return servers
}

func (cluster *mongoCluster) removeServer(server *mongoServer) {
	cluster.Lock()
	cluster.masters.Remove(server)
	other := cluster.servers.Remove(server)
	cluster.Unlock()
	if other != nil {
		other.Close()
	}
	server.Close()
}

type isMasterResult struct {
	IsMaster       bool
	Secondary      bool
	Primary        string
	Hosts          []string
	Passives       []string
	Tags           bson.D
	Msg            string
	SetName        string `bson:"setName"`
	MaxWireVersion int    `bson:"maxWireVersion"`
}

func (cluster *mongoCluster) isMaster(socket *mongoSocket, result *isMasterResult) error {
	session := newSession(Monotonic, cluster, 10*time.Second)
	session.setSocket(socket)
	err := session.Run("ismaster", result)
	session.Close()
	return err
}

type possibleTimeout interface {
	Timeout() bool
}

var syncSocketTimeout = 5 * time.Second

func (cluster *mongoCluster) syncServer(server *mongoServer) (info *mongoServerInfo, hosts []string, err error) {
	var syncTimeout = syncSocketTimeout

	addr := server.Addr

	var result isMasterResult
	var tryerr error
	for retry := 0; ; retry++ {
		if retry == 3 || retry == 1 && cluster.failFast {
			return nil, nil, tryerr
		}
		if retry > 0 {
			if err, ok := tryerr.(possibleTimeout); ok && err.Timeout() {
				cluster.serverSynced.Broadcast()
			}
			time.Sleep(syncShortDelay)
		}

		socket, _, err := server.AcquireSocket(0, syncTimeout)
		if err != nil {
			tryerr = err
			continue
		}
		err = cluster.isMaster(socket, &result)
		socket.Release()
		if err != nil {
			tryerr = err
			continue
		}
		break
	}

	if cluster.setName != "" && result.SetName != cluster.setName {
		return nil, nil, fmt.Errorf("server %s is not a member of replica set %q", addr, cluster.setName)
	}

	if result.IsMaster {
		if !server.info.Master {
			stats.conn(-1, false)
			stats.conn(+1, true)
		}
	} else if result.Secondary {
	} else if cluster.direct {
	} else {
		return nil, nil, errors.New(addr + " is not a master nor slave")
	}

	info = &mongoServerInfo{
		Master:         result.IsMaster,
		Mongos:         result.Msg == "isdbgrid",
		Tags:           result.Tags,
		SetName:        result.SetName,
		MaxWireVersion: result.MaxWireVersion,
	}

	hosts = make([]string, 0, 1+len(result.Hosts)+len(result.Passives))
	if result.Primary != "" {
		hosts = append(hosts, result.Primary)
	}
	hosts = append(hosts, result.Hosts...)
	hosts = append(hosts, result.Passives...)

	return info, hosts, nil
}

type syncKind bool

const (
	completeSync syncKind = true
	partialSync  syncKind = false
)

func (cluster *mongoCluster) addServer(server *mongoServer, info *mongoServerInfo, syncKind syncKind) {
	cluster.Lock()
	current := cluster.servers.Search(server.ResolvedAddr)
	if current == nil {
		if syncKind == partialSync {
			cluster.Unlock()
			server.Close()
			return
		}
		cluster.servers.Add(server)
		if info.Master {
			cluster.masters.Add(server)
		}
	} else {
		if server != current {
			panic("addServer attempting to add duplicated server")
		}
		if server.Info().Master != info.Master {
			if info.Master {
				cluster.masters.Add(server)
			} else {
				cluster.masters.Remove(server)
			}
		}
	}
	server.SetInfo(info)
	cluster.serverSynced.Broadcast()
	cluster.Unlock()
}

func (cluster *mongoCluster) getKnownAddrs() []string {
	cluster.RLock()
	max := len(cluster.userSeeds) + len(cluster.dynaSeeds) + cluster.servers.Len()
	seen := make(map[string]bool, max)
	known := make([]string, 0, max)

	add := func(addr string) {
		if _, found := seen[addr]; !found {
			seen[addr] = true
			known = append(known, addr)
		}
	}

	for _, addr := range cluster.userSeeds {
		add(addr)
	}
	for _, addr := range cluster.dynaSeeds {
		add(addr)
	}
	for _, serv := range cluster.servers.Slice() {
		add(serv.Addr)
	}
	cluster.RUnlock()

	return known
}

func (cluster *mongoCluster) syncServers() {
	select {
	case cluster.sync <- true:
	default:
	}
}

const syncServersDelay = 30 * time.Second
const syncShortDelay = 500 * time.Millisecond

func (cluster *mongoCluster) syncServersLoop() {
	for {
		cluster.Lock()
		if cluster.references == 0 {
			cluster.Unlock()
			break
		}
		cluster.references++
		direct := cluster.direct
		cluster.Unlock()

		cluster.syncServersIteration(direct)

		select {
		case <-cluster.sync:
		default:
		}

		cluster.Release()
		if !cluster.failFast {
			time.Sleep(syncShortDelay)
		}

		cluster.Lock()
		if cluster.references == 0 {
			cluster.Unlock()
			break
		}
		cluster.syncCount++
		cluster.serverSynced.Broadcast()
		restart := !direct && cluster.masters.Empty() || cluster.servers.Empty()
		cluster.Unlock()

		if restart {
			time.Sleep(syncShortDelay)
			continue
		}

		select {
		case <-cluster.sync:
		case <-time.After(syncServersDelay):
		}
	}
}

func (cluster *mongoCluster) server(addr string, tcpaddr *net.TCPAddr) *mongoServer {
	cluster.RLock()
	server := cluster.servers.Search(tcpaddr.String())
	cluster.RUnlock()
	if server != nil {
		return server
	}
	return newServer(addr, tcpaddr, cluster.sync, cluster.dial)
}

func resolveAddr(addr string) (*net.TCPAddr, error) {
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if port, _ := strconv.Atoi(port); port > 0 {
			zone := ""
			if i := strings.LastIndex(host, "%"); i >= 0 {
				zone = host[i+1:]
				host = host[:i]
			}
			ip := net.ParseIP(host)
			if ip != nil {
				return &net.TCPAddr{IP: ip, Port: port, Zone: zone}, nil
			}
		}
	}

	addrChan := make(chan *net.TCPAddr, 2)
	for _, network := range []string{"udp4", "udp6"} {
		network := network
		go func() {

			conn, err := net.DialTimeout(network, addr, 10*time.Second)
			if err != nil {
				addrChan <- nil
			} else {
				addrChan <- (*net.TCPAddr)(conn.RemoteAddr().(*net.UDPAddr))
				conn.Close()
			}
		}()
	}

	tcpaddr := <-addrChan
	if tcpaddr == nil || len(tcpaddr.IP) != 4 {
		var timeout <-chan time.Time
		if tcpaddr != nil {
			timeout = time.After(50 * time.Millisecond)
		}
		select {
		case <-timeout:
		case tcpaddr2 := <-addrChan:
			if tcpaddr == nil || tcpaddr2 != nil {
				tcpaddr = tcpaddr2
			}
		}
	}

	if tcpaddr == nil {
		return nil, errors.New("failed to resolve server address: " + addr)
	}

	return tcpaddr, nil
}

type pendingAdd struct {
	server *mongoServer
	info   *mongoServerInfo
}

func (cluster *mongoCluster) syncServersIteration(direct bool) {
	var wg sync.WaitGroup
	var m sync.Mutex
	notYetAdded := make(map[string]pendingAdd)
	addIfFound := make(map[string]bool)
	seen := make(map[string]bool)
	syncKind := partialSync

	var spawnSync func(addr string, byMaster bool)
	spawnSync = func(addr string, byMaster bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tcpaddr, err := resolveAddr(addr)
			if err != nil {
				return
			}
			resolvedAddr := tcpaddr.String()

			m.Lock()
			if byMaster {
				if pending, ok := notYetAdded[resolvedAddr]; ok {
					delete(notYetAdded, resolvedAddr)
					m.Unlock()
					cluster.addServer(pending.server, pending.info, completeSync)
					return
				}
				addIfFound[resolvedAddr] = true
			}
			if seen[resolvedAddr] {
				m.Unlock()
				return
			}
			seen[resolvedAddr] = true
			m.Unlock()

			server := cluster.server(addr, tcpaddr)
			info, hosts, err := cluster.syncServer(server)
			if err != nil {
				cluster.removeServer(server)
				return
			}

			m.Lock()
			add := direct || info.Master || addIfFound[resolvedAddr]
			if add {
				syncKind = completeSync
			} else {
				notYetAdded[resolvedAddr] = pendingAdd{server, info}
			}
			m.Unlock()
			if add {
				cluster.addServer(server, info, completeSync)
			}
			if !direct {
				for _, addr := range hosts {
					spawnSync(addr, info.Master)
				}
			}
		}()
	}

	knownAddrs := cluster.getKnownAddrs()
	for _, addr := range knownAddrs {
		spawnSync(addr, false)
	}
	wg.Wait()

	if syncKind == completeSync {
		for _, pending := range notYetAdded {
			cluster.removeServer(pending.server)
		}
	} else {
		for _, pending := range notYetAdded {
			cluster.addServer(pending.server, pending.info, partialSync)
		}
	}

	cluster.Lock()
	if syncKind == completeSync {
		dynaSeeds := make([]string, cluster.servers.Len())
		for i, server := range cluster.servers.Slice() {
			dynaSeeds[i] = server.Addr
		}
		cluster.dynaSeeds = dynaSeeds
	}
	cluster.Unlock()
}

func (cluster *mongoCluster) AcquireSocket(mode Mode, slaveOk bool, syncTimeout time.Duration, socketTimeout time.Duration, serverTags []bson.D, poolLimit int) (s *mongoSocket, err error) {
	var started time.Time
	var syncCount uint
	warnedLimit := false
	for {
		cluster.RLock()
		for {
			mastersLen := cluster.masters.Len()
			slavesLen := cluster.servers.Len() - mastersLen
			if mastersLen > 0 && !(slaveOk && mode == Secondary) || slavesLen > 0 && slaveOk {
				break
			}
			if mastersLen > 0 && mode == Secondary && cluster.masters.HasMongos() {
				break
			}
			if started.IsZero() {
				started = time.Now()
				syncCount = cluster.syncCount
			} else if syncTimeout != 0 && started.Before(time.Now().Add(-syncTimeout)) || cluster.failFast && cluster.syncCount != syncCount {
				cluster.RUnlock()
				return nil, errors.New("no reachable servers")
			}

			cluster.syncServers()
			cluster.serverSynced.Wait()
		}

		var server *mongoServer
		if slaveOk {
			server = cluster.servers.BestFit(mode, serverTags)
		} else {
			server = cluster.masters.BestFit(mode, nil)
		}
		cluster.RUnlock()

		if server == nil {
			time.Sleep(1e8)
			continue
		}

		s, abended, err := server.AcquireSocket(poolLimit, socketTimeout)
		if err == errPoolLimit {
			if !warnedLimit {
				warnedLimit = true
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err != nil {
			cluster.removeServer(server)
			cluster.syncServers()
			continue
		}
		if abended && !slaveOk {
			var result isMasterResult
			err := cluster.isMaster(s, &result)
			if err != nil || !result.IsMaster {
				s.Release()
				cluster.syncServers()
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		return s, nil
	}

	panic("unreached")
}

func (cluster *mongoCluster) CacheIndex(cacheKey string, exists bool) {
	cluster.Lock()
	if cluster.cachedIndex == nil {
		cluster.cachedIndex = make(map[string]bool)
	}
	if exists {
		cluster.cachedIndex[cacheKey] = true
	} else {
		delete(cluster.cachedIndex, cacheKey)
	}
	cluster.Unlock()
}

func (cluster *mongoCluster) HasCachedIndex(cacheKey string) (result bool) {
	cluster.RLock()
	if cluster.cachedIndex != nil {
		result = cluster.cachedIndex[cacheKey]
	}
	cluster.RUnlock()
	return
}

func (cluster *mongoCluster) ResetIndexCache() {
	cluster.Lock()
	cluster.cachedIndex = make(map[string]bool)
	cluster.Unlock()
}
