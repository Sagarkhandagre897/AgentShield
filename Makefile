# AgentShield — module path and toolchain
MODULE := github.com/Sagarkhandagre897/AgentShield
PROTO  := proto/agentshield/v1/agentshield.proto

.PHONY: all proto tidy build test run fmt vet clean

all: build

## proto: regenerate Go gRPC code from the .proto contract
proto:
	protoc \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO)

## tidy: resolve and prune module dependencies
tidy:
	go mod tidy

## build: compile everything
build:
	go build ./...

## test: run the full test suite
test:
	go test ./...

## vet: static checks
vet:
	go vet ./...

## fmt: format the tree
fmt:
	gofmt -w .

## run: start the decision service (added in the decision-service commit)
run:
	go run ./cmd/decision

## clean: remove build artifacts
clean:
	rm -rf bin coverage.*
