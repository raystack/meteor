NAME="github.com/odpf/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"

.PHONY: all build clean test

all: build

build:
	go build -ldflags "-X main.Version=${VERSION}" ${NAME}

clean:
	rm -rf dist/

test:
	go test ./... -coverprofile=coverage.out

test-coverage: test
	go tool cover -html=coverage.out

generate-proto: ## regenerate protos
	@echo " > cloning protobuf from odpf/proton"
	@echo " > generating protobuf"
	@buf generate --template buf.gen.yaml https://github.com/odpf/proton/archive/52353ad461321cb601b6c963c26f0ee50e0d398b.zip#strip_components=1 --path odpf/assets
	@echo " > protobuf compilation finished"