package api

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/sricharanraj/distributed-kv-store/internal/storage"
)

// TCPServer implements a small, line-based, Redis-inspired protocol on top
// of the same Server (and therefore the same sharding/replication logic as
// the HTTP API). Supported commands:
//
//	SET <key> <value...>   -> +OK
//	GET <key>              -> $<value>  or  $-1 (missing)
//	DEL <key>               -> +OK
//	PING                    -> +PONG
//	QUIT                    -> closes the connection
type TCPServer struct {
	srv *Server
}

func NewTCPServer(srv *Server) *TCPServer {
	return &TCPServer{srv: srv}
}

func (t *TCPServer) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("tcp protocol server listening on %s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go t.handleConn(conn)
	}
}

func (t *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		resp := t.dispatch(line)
		fmt.Fprintf(conn, "%s\r\n", resp)
	}
}

func (t *TCPServer) dispatch(line string) string {
	parts := strings.SplitN(line, " ", 3)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "PING":
		return "+PONG"

	case "QUIT":
		return "+OK"

	case "GET":
		if len(parts) < 2 {
			return "-ERR usage: GET <key>"
		}
		key := parts[1]
		val, err := t.get(key)
		if err == storage.ErrNotFound {
			return "$-1"
		}
		if err != nil {
			return "-ERR " + err.Error()
		}
		return "$" + string(val)

	case "SET":
		if len(parts) < 3 {
			return "-ERR usage: SET <key> <value>"
		}
		if err := t.put(parts[1], []byte(parts[2])); err != nil {
			return "-ERR " + err.Error()
		}
		return "+OK"

	case "DEL":
		if len(parts) < 2 {
			return "-ERR usage: DEL <key>"
		}
		if err := t.del(parts[1]); err != nil {
			return "-ERR " + err.Error()
		}
		return "+OK"

	default:
		return "-ERR unknown command '" + cmd + "'"
	}
}

func (t *TCPServer) get(key string) ([]byte, error) {
	if t.srv.isOwner(key) {
		return t.srv.Engine.Get(key)
	}
	primary := t.srv.owners(key)[0]
	body, status, err := t.srv.Replicator.FetchGet(context.Background(), primary, key)
	if err != nil {
		return nil, err
	}
	if status == 404 {
		return nil, storage.ErrNotFound
	}
	return body, nil
}

func (t *TCPServer) put(key string, value []byte) error {
	owners := t.srv.owners(key)
	if !t.srv.isOwner(key) {
		return t.srv.Replicator.PushPut(context.Background(), owners[0], key, value)
	}
	if err := t.srv.Engine.Put(key, value); err != nil {
		return err
	}
	t.srv.replicateAsync(context.Background(), owners, key, value, false)
	return nil
}

func (t *TCPServer) del(key string) error {
	owners := t.srv.owners(key)
	if !t.srv.isOwner(key) {
		return t.srv.Replicator.PushDelete(context.Background(), owners[0], key)
	}
	if err := t.srv.Engine.Delete(key); err != nil {
		return err
	}
	t.srv.replicateAsync(context.Background(), owners, key, nil, true)
	return nil
}
