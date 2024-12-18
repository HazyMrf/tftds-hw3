package crdt

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

func (c *CRDT) handleOperation(op Operation) {
	if c.isOperationLater(op) {
		if op.Type == OperationAdd {
			c.data[op.Key] = op.Value
		} else if op.Type == OperationRemove {
			delete(c.data, op.Key)
		}
		if _, exists := c.timestamps[op.Key]; !exists {
			c.timestamps[op.Key] = make(map[string]int)
		}
		for replica, ts := range op.Timestamp {
			c.timestamps[op.Key][replica] = ts
		}
		c.history = append(c.history, op)
	}
}

func CopyTimestamps(original map[string]int) map[string]int {
	copy := make(map[string]int)
	for k, v := range original {
		copy[k] = v
	}
	return copy
}

func respondError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func decodeJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func (c *CRDT) PatchHandler(w http.ResponseWriter, r *http.Request) {
	var updates map[string]string
	if err := decodeJSON(r, &updates); err != nil {
		respondError(w, "invalid request", http.StatusBadRequest)
		return
	}

	slog.Info("Patch request", "replicaID", c.replicaID, "updates", updates)

	c.lock.Lock()
	defer c.lock.Unlock()

	for key, value := range updates {
		c.incrementLocalClock()
		opType := OperationAdd
		if value == "" {
			opType = OperationRemove
		}
		op := Operation{
			Key:       key,
			Value:     value,
			Timestamp: CopyTimestamps(c.vector),
			Type:      opType,
			ReplicaID: c.replicaID,
		}
		c.handleOperation(op)
	}

	w.WriteHeader(http.StatusOK)
}

func (c *CRDT) SyncHandler(w http.ResponseWriter, r *http.Request) {
	var operations []Operation
	if err := decodeJSON(r, &operations); err != nil {
		respondError(w, "invalid request", http.StatusBadRequest)
		return
	}

	slog.Info("Sync request", "replicaID", c.replicaID, "operations", operations)

	c.lock.Lock()
	defer c.lock.Unlock()

	for _, op := range operations {
		c.handleOperation(op)
		c.mergeVector(op.Timestamp)
	}

	w.WriteHeader(http.StatusOK)
}
