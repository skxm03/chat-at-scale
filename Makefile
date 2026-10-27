.PHONY: proto infra-up infra-down infra-logs build test clean

export PATH := $(PATH):$(shell go env GOPATH)/bin

proto:
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/proto/messaging/v1/messaging.proto

infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-logs:
	docker compose logs -f

init-db:
	docker exec -i chat-cassandra cqlsh < scripts/schema.cql

build:
	go build ./...

test:
	go test -v ./...

clean:
	rm -rf bin/

