.PHONY: build test run dev clean

build:
	go build -o bin/ucanlog ./cmd/ucanlog

test:
	go test -v -timeout=5m ./...

test-integration:
	go test -v -timeout=10m -tags=integration ./...

run: build
	./bin/ucanlog

dev:
	go run ./cmd/ucanlog

clean:
	rm -rf bin/

lint:
	golangci-lint run

deps:
	go mod tidy
	go mod download