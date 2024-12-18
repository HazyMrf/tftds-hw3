package main

import (
	crdt "crdt/src"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/lmittmann/tint"
)

func initLogger() {
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelInfo,
		}),
	))
}

func parseFlags() (string, int, []string) {
	var (
		replicaID string
		port      int
		peers     []string
	)

	flag.StringVar(&replicaID, "id", "", "Id for replicaset")
	flag.IntVar(&port, "port", 0, "HTTP-port for replica")
	flag.Parse()
	peers = flag.Args()

	return replicaID, port, peers
}

func setupHTTPHandlers(handler *crdt.CRDT) {
	http.HandleFunc("/patch", handler.PatchHandler)
	http.HandleFunc("/sync", handler.SyncHandler)
}

func main() {
	initLogger()
	replicaID, port, peers := parseFlags()

	crdt := crdt.InitCRDT(replicaID, peers)
	setupHTTPHandlers(crdt)

	slog.Info("Server is ready.", "ReplicaID:", replicaID, "Port:", port, "Peers:", peers)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
