.PHONY: build test bench vet cluster clean

build:
	go build -o bin/server ./cmd/server
	go build -o bin/kvctl ./cmd/kvctl

test:
	go test ./... -race -count=1

bench:
	go test ./benchmark/... -bench=. -benchmem

vet:
	go vet ./...

cluster:
	./scripts/run_cluster.sh

clean:
	rm -rf bin data-node1 data-node2 data-node3
