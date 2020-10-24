package mgo

import (
	"sync"
)

var stats *Stats
var statsMutex sync.Mutex

type Stats struct {
	Clusters     int
	MasterConns  int
	SlaveConns   int
	SentOps      int
	ReceivedOps  int
	ReceivedDocs int
	SocketsAlive int
	SocketsInUse int
	SocketRefs   int
}

func (stats *Stats) cluster(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.Clusters += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) conn(delta int, master bool) {
	if stats != nil {
		statsMutex.Lock()
		if master {
			stats.MasterConns += delta
		} else {
			stats.SlaveConns += delta
		}
		statsMutex.Unlock()
	}
}

func (stats *Stats) sentOps(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.SentOps += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) receivedOps(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.ReceivedOps += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) receivedDocs(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.ReceivedDocs += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) socketsInUse(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.SocketsInUse += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) socketsAlive(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.SocketsAlive += delta
		statsMutex.Unlock()
	}
}

func (stats *Stats) socketRefs(delta int) {
	if stats != nil {
		statsMutex.Lock()
		stats.SocketRefs += delta
		statsMutex.Unlock()
	}
}
