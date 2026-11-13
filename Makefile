PG ?= postgres://postgres:herald@127.0.0.1:5432/postgres

.PHONY: test test-pg dev lint

test:
	go test -race ./...

test-pg:
	HERALD_PG=$(PG) go test -race -count=1 ./...

dev:
	go run ./cmd/herald-dev

lint:
	go vet ./... && test -z "$$(gofmt -l .)"
