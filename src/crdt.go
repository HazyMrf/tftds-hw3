package crdt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Properties of CRDT operations:
// * Commutative
// * Idempotent
// * Associative

type OperationType string

const (
	OperationAdd    OperationType = "add"
	OperationRemove OperationType = "remove"
)

type Operation struct {
	Key       string         `json:"key"`
	Value     string         `json:"value,omitempty"`
	Type      OperationType  `json:"type"`
	Timestamp map[string]int `json:"timestamp"`
	ReplicaID string         `json:"replica_id"`
}

type CRDT struct {
	data       map[string]string         // current key-value pairs
	timestamps map[string]map[string]int // per-key vector timestamps
	lock       sync.RWMutex
	history    []Operation    // record of applied operations
	replicaID  string         // unique ID of this replica
	vector     map[string]int // vector clock for this replica

	peers             []string
	heartbeatInterval time.Duration
	hbTimer           *time.Timer
}

func (crdt *CRDT) sendSyncRequest(peer string) {
	slog.Info("Initiating sync", "from replicaID", crdt.replicaID, "to replicaID", peer)

	crdt.lock.RLock()
	payload, err := json.Marshal(crdt.history)
	crdt.lock.RUnlock()
	if err != nil {
		slog.Error("Failed to encode history", "error", err)
		return
	}

	url := fmt.Sprintf("http://%s/sync", peer)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		slog.Error("Sync request failed", "from replicaID", crdt.replicaID, "to replicaID", peer, "error", err)
		return
	}
	defer resp.Body.Close()
}

func computeHash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func (crdt *CRDT) triggerHeartbeat() {
	slog.Info("Heartbeat triggered", "replicaID", crdt.replicaID)
	for _, peer := range crdt.peers {
		go crdt.sendSyncRequest(peer)
	}
	crdt.hbTimer = time.AfterFunc(crdt.heartbeatInterval, crdt.triggerHeartbeat)
}

// TESTING
func (crdt *CRDT) TESTHeartbeat() {
	crdt.triggerHeartbeat()
}

func InitCRDT(replicaID string, clusterPeers []string) *CRDT {
	instance := &CRDT{
		data:       make(map[string]string),
		timestamps: make(map[string]map[string]int),
		history:    []Operation{},
		replicaID:  replicaID,
		vector:     map[string]int{replicaID: 0},

		peers:             clusterPeers,
		heartbeatInterval: 10 * time.Second,
	}
	instance.hbTimer = time.AfterFunc(instance.heartbeatInterval, instance.triggerHeartbeat)
	return instance
}

func (crdt *CRDT) incrementLocalClock() {
	crdt.vector[crdt.replicaID]++
}

func (crdt *CRDT) mergeVector(remoteVector map[string]int) {
	for rep, ts := range remoteVector {
		if current, known := crdt.vector[rep]; !known || ts > current {
			crdt.vector[rep] = ts
		}
	}
}

// Clocks comparison
func (crdt *CRDT) isOperationLater(op Operation) bool {
	localKeyTs := crdt.timestamps[op.Key]

	// If we have no record of this key, assume incoming operation is newer.
	if localKeyTs == nil {
		return true
	}

	// Compare incoming timestamps to local timestamps.

	hasGreater, hasSmaller := false, false
	for replicaID, incTS := range op.Timestamp {
		localTS, exists := localKeyTs[replicaID]
		switch {
		case !exists || incTS > localTS:
			hasGreater = true
		case incTS < localTS:
			hasSmaller = true
		}

		if hasGreater && hasSmaller {
			break
		}
	}

	if !hasSmaller {
		for replicaID := range localKeyTs {
			if _, exists := op.Timestamp[replicaID]; !exists {
				hasSmaller = true
				break
			}
		}
	}

	// Decider

	switch {
	case hasGreater && !hasSmaller:
		return true
	case hasSmaller && !hasGreater, !hasGreater && !hasSmaller:
		return false
	default:
		// Conflict case: tie-break via hash.
		return computeHash(op.ReplicaID)%2 == 0
	}
}
