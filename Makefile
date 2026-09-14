.PHONY: run build test race lint clean

build:
	go build -o bin/mini-redis-go ./cmd/server

run: build
	./bin/mini-redis-go

test:
	go test ./...

race:
	go test ./... -race

lint:
	go vet ./...

clean:
	rm -rf bin/
