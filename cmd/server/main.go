// Command server runs a single distributed-kv-store node: it owns a shard
// of the key space (determined by consistent hashing over the cluster's
// membership list), serves the REST API over HTTP and a mini-Redis text
// protocol over TCP, and replicates writes to the other replica owners.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sricharanraj/distributed-kv-store/internal/api"
	"github.com/sricharanraj/distributed-kv-store/internal/cluster"
	"github.com/sricharanraj/distributed-kv-store/internal/storage"
)

func main() {
	var (
		httpAddr  = flag.String("http-addr", "127.0.0.1:8080", "address for the HTTP REST API")
		tcpAddr   = flag.String("tcp-addr", "127.0.0.1:6380", "address for the TCP text protocol")
		selfAddr  = flag.String("node-id", "", "this node's ID as known to peers (defaults to -http-addr)")
		peers     = flag.String("peers", "", "comma-separated list of peer node IDs (their -http-addr values)")
		dataDir   = flag.String("data-dir", "data", "directory for WAL + SSTable files")
		replicas  = flag.Int("replicas", 1, "replication factor (number of nodes each key is stored on)")
		flushMB   = flag.Int("flush-mb", 4, "memtable flush threshold in MB")
	)
	flag.Parse()

	self := *selfAddr
	if self == "" {
		self = *httpAddr
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	cfg := storage.DefaultConfig(*dataDir)
	cfg.MemtableFlushBytes = *flushMB << 20

	engine, err := storage.Open(cfg)
	if err != nil {
		log.Fatalf("open storage engine: %v", err)
	}
	defer engine.Close()

	membership := cluster.NewMembership(self, peerList)
	rf := *replicas
	if rf > len(membership.Ring.Nodes()) {
		rf = len(membership.Ring.Nodes())
	}
	server := api.NewServer(engine, membership, rf)

	httpSrv := &http.Server{Addr: *httpAddr, Handler: server.Handler()}
	tcpSrv := api.NewTCPServer(server)

	go func() {
		log.Printf("node %s: http REST API listening on %s", self, *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	go func() {
		if err := tcpSrv.ListenAndServe(*tcpAddr); err != nil {
			log.Fatalf("tcp server: %v", err)
		}
	}()

	log.Printf("node %s: cluster nodes=%v replication_factor=%d", self, membership.Ring.Nodes(), rf)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
}
