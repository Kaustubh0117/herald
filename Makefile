PG ?= postgres://postgres:herald@127.0.0.1:5432/postgres

.PHONY: test test-pg dev lint proto

test:
	go test -race ./...

test-pg:
	HERALD_PG=$(PG) go test -race -count=1 ./...

dev:
	go run ./cmd/herald-dev -pg $(PG)

lint:
	go vet ./... && test -z "$$(gofmt -l .)"

proto:
	PATH=$$PATH:$$(go env GOPATH)/bin protoc --go_out=. --go_opt=module=github.com/Kaustubh0117/herald \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Kaustubh0117/herald proto/herald.proto
