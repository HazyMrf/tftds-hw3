package crdt_test

import (
	"bytes"
	"context"
	crdt "crdt/src"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lmittmann/tint"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

const basePort = 8080

type TestCRDTServer struct {
	httpServeMux *http.ServeMux
	httpServer   *http.Server
	crdtServer   *crdt.CRDT
	address      string
}

// Helpers

func initLogger() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelInfo,
		}),
	))
}

func randomKeyValue() map[string]string {
	key := fmt.Sprintf("key%d", rand.Intn(5))
	value := fmt.Sprintf("value%d", rand.Intn(1000))
	return map[string]string{key: value}
}

func (t *TestCRDTServer) sendPatchRequest(updates map[string]string) error {
	data, err := json.Marshal(updates)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://localhost%s/patch", t.address)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// Getters

func (t *TestCRDTServer) sendHeartbeat() {
	t.crdtServer.TESTHeartbeat()
}

func (t *TestCRDTServer) GetHistory() []crdt.Operation {
	return t.crdtServer.GetHistory()
}

func (t *TestCRDTServer) GetTimestamps() map[string]map[string]int {
	return t.crdtServer.GetTimestamps()
}

func (t *TestCRDTServer) GetValue(key string) string {
	return t.crdtServer.GetValue(key)
}

func (t *TestCRDTServer) GetData() map[string]string {
	return t.crdtServer.GetData()
}

// Init/Shutdown of Test

func (t *TestCRDTServer) StartTestServer() {
	t.httpServer = &http.Server{
		Addr:    t.address,
		Handler: t.httpServeMux,
	}
	go func() {
		slog.Info("Starting server...", "address", t.address)
		if err := t.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "address", t.address, "error", err)
		}
	}()
}

func (t *TestCRDTServer) StopTestServer() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	t.httpServer.Shutdown(ctx)
}

func InitTestServers(t *testing.T, count int) []*TestCRDTServer {
	initLogger()

	allPeers := make([]string, count)
	for i := 0; i < count; i++ {
		allPeers[i] = fmt.Sprintf(":%d", basePort+i)
	}

	servers := make([]*TestCRDTServer, 0, count)
	for _, addr := range allPeers {
		var peers []string
		for _, p := range allPeers {
			if p != addr {
				peers = append(peers, p)
			}
		}

		crdtInst := crdt.InitCRDT(addr, peers)
		mux := http.NewServeMux()
		mux.HandleFunc("/patch", crdtInst.PatchHandler)
		mux.HandleFunc("/sync", crdtInst.SyncHandler)

		testServer := &TestCRDTServer{
			crdtServer:   crdtInst,
			address:      addr,
			httpServeMux: mux,
		}
		testServer.StartTestServer()

		t.Cleanup(func() {
			testServer.StopTestServer()
		})

		servers = append(servers, testServer)
	}
	return servers
}

// TESTS

func TestSyncAllNodes(t *testing.T) {
	servers := InitTestServers(t, 3)

	for i, s := range servers {
		err := s.sendPatchRequest(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)

		err = s.sendPatchRequest(map[string]string{
			fmt.Sprint((i + 1) * 10): fmt.Sprint((i+1)*10 + 1),
		})
		assert.NoError(t, err)
	}

	for _, s := range servers {
		s.sendHeartbeat()
	}

	time.Sleep(1 * time.Second)

	expectedData := map[string]string{"0": "1", "1": "2", "2": "3", "10": "11", "20": "21", "30": "31"}
	for _, s := range servers {
		assert.Equal(t, 6, len(s.GetHistory()))
		assert.Equal(t, expectedData, s.GetData())
	}
}

func TestRecovery(t *testing.T) {
	servers := InitTestServers(t, 3)

	servers[2].StopTestServer()

	for i, s := range servers {
		if i == 2 {
			continue
		}
		err := s.sendPatchRequest(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)
	}

	for _, s := range servers {
		s.sendHeartbeat()
	}

	servers[2].StartTestServer()

	for _, s := range servers {
		s.sendHeartbeat()
	}

	time.Sleep(1 * time.Second)

	expectedData := map[string]string{"0": "1", "1": "2"}
	for _, s := range servers {
		assert.Equal(t, 2, len(s.GetHistory()))
		assert.Equal(t, expectedData, s.GetData())
	}
}

func TestPatchConflict(t *testing.T) {
	servers := InitTestServers(t, 3)

	assert.NoError(t, servers[0].sendPatchRequest(map[string]string{"1": "2"}))
	assert.NoError(t, servers[1].sendPatchRequest(map[string]string{"1": "3"}))

	for _, s := range servers {
		s.sendHeartbeat()
	}

	time.Sleep(1 * time.Second)

	for i, s := range servers {
		if i == 0 {
			assert.Equal(t, 2, len(s.GetHistory()))
		} else if i == 1 {
			assert.Equal(t, 1, len(s.GetHistory()))
		}
		assert.Equal(t, map[string]string{"1": "3"}, s.GetData())
	}
}
func TestBig(t *testing.T) {
	servers := InitTestServers(t, 3)
	rand.Seed(uint64(time.Now().UnixNano()))

	const (
		opsCount   = 1000
		maxRetries = 5
	)

	patchWithRetries := func(srv *TestCRDTServer, patch map[string]string) {
		for i := 0; i < maxRetries; i++ {
			if err := srv.sendPatchRequest(patch); err == nil {
				return
			}
			if i == maxRetries-1 {
				slog.Error("Failed to patch server after retries")
				os.Exit(1)
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(opsCount)

	for i := 0; i < opsCount; i++ {
		go func() {
			defer wg.Done()
			server := servers[rand.Intn(len(servers))]
			patchWithRetries(server, randomKeyValue())
		}()
	}

	wg.Wait()

	for _, s := range servers {
		s.sendHeartbeat()
	}

	time.Sleep(3 * time.Second)

	expectedData := servers[0].GetData()
	for _, s := range servers[1:] {
		assert.Equal(t, expectedData, s.GetData())
	}
}
