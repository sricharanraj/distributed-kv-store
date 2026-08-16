// Command kvctl is a small CLI client for the distributed-kv-store REST API.
//
// Usage:
//
//	kvctl -addr 127.0.0.1:8080 put <key> <value>
//	kvctl -addr 127.0.0.1:8080 get <key>
//	kvctl -addr 127.0.0.1:8080 del <key>
//	kvctl -addr 127.0.0.1:8080 scan [prefix]
//	kvctl -addr 127.0.0.1:8080 status
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address of a cluster node's HTTP API")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	base := "http://" + *addr
	client := &http.Client{}

	switch args[0] {
	case "put":
		if len(args) < 3 {
			usage()
			os.Exit(1)
		}
		doPut(client, base, args[1], args[2])
	case "get":
		if len(args) < 2 {
			usage()
			os.Exit(1)
		}
		doGet(client, base, args[1])
	case "del":
		if len(args) < 2 {
			usage()
			os.Exit(1)
		}
		doDelete(client, base, args[1])
	case "scan":
		prefix := ""
		if len(args) >= 2 {
			prefix = args[1]
		}
		doScan(client, base, prefix)
	case "status":
		doStatus(client, base)
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kvctl [-addr host:port] <put|get|del|scan|status> [args...]")
}

func doPut(client *http.Client, base, key, value string) {
	req, _ := http.NewRequest(http.MethodPut, base+"/kv/"+key, bytes.NewBufferString(value))
	resp, err := client.Do(req)
	fatalIf(err)
	defer resp.Body.Close()
	printResponse(resp)
}

func doGet(client *http.Client, base, key string) {
	resp, err := client.Get(base + "/kv/" + key)
	fatalIf(err)
	defer resp.Body.Close()
	printResponse(resp)
}

func doDelete(client *http.Client, base, key string) {
	req, _ := http.NewRequest(http.MethodDelete, base+"/kv/"+key, nil)
	resp, err := client.Do(req)
	fatalIf(err)
	defer resp.Body.Close()
	printResponse(resp)
}

func doScan(client *http.Client, base, prefix string) {
	resp, err := client.Get(base + "/kv?prefix=" + prefix)
	fatalIf(err)
	defer resp.Body.Close()
	var out map[string]string
	fatalIf(json.NewDecoder(resp.Body).Decode(&out))
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}

func doStatus(client *http.Client, base string) {
	resp, err := client.Get(base + "/cluster/status")
	fatalIf(err)
	defer resp.Body.Close()
	printResponse(resp)
}

func printResponse(resp *http.Response) {
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "error (%d): %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
