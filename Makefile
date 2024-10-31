NAME="github.com/raystack/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"
PROTON_COMMIT := "ae895e033f71df187c62d7cf9431a2e259ddd423"
.PHONY: all build clean test

all: build

build:
	go build -ldflags "-X ${NAME}/cmd.Version=${VERSION}" ${NAME}

build-dev:
	CGO_ENABLED=0 go build -ldflags "-X ${NAME}/cmd.Version=dev" ${NAME}

clean:
	rm -rf dist/

copy-config:
	cp ./config/meteor.yaml.sample ./meteor.yaml

test:
	go test $(shell go list ./... | grep -v 'test\|mocks\|plugins\|v1beta2\|cmd') -coverprofile=coverage.out

test-e2e:
	go test ./test/e2e -tags=integration -count=1

test-plugins:
	@echo " > Testing plugins with tag 'plugins'"
	go test $(if $(filter .,$(PLUGIN)),./plugins,$(if $(PLUGIN),./plugins/$(PLUGIN)/...,./plugins/...)) -tags=plugins -coverprofile=coverage-plugins$(subst .,root,$(subst /,-,$(if $(PLUGIN),-$(PLUGIN),))).out -parallel=1

test-coverage:
	cp coverage.out coverage-all.out
	tail -n +2 coverage-plugins.out >> coverage-all.out
	go tool cover -html=coverage-all.out

generate-proto:
	@echo " > cloning protobuf from raystack/proton"
	@echo " > generating protobuf"
	@buf generate --template buf.gen.yaml https://github.com/raystack/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --path raystack/assets/v1beta2
	@echo " > protobuf compilation finished"

lint:
	golangci-lint run

install:
	@echo "> installing dependencies"
	go install github.com/vektra/mockery/v2@v2.14.0
