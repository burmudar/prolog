SHELL :=/usr/bin/env zsh

deps:
	go get google.golang.org/grpc@v1.32.0
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.0.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.0.0

compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

test:
	go test -race ./...
