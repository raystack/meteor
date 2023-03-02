NAME="github.com/odpf/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"
PROTON_COMMIT := "8990712599f715240a3fbe9ce034a204b4a32245"
.PHONY: all build clean test

all: build

build:
	go build -ldflags "-X cmd.Version=${VERSION}" ${NAME}

build-dev:
	CGO_ENABLED=0 go build -ldflags "-X cmd.Version=dev" ${NAME}

clean:
	rm -rf dist/

copy-config:
	cp ./config/meteor.yaml.sample ./meteor.yaml

test:
	go test ./... -coverprofile=coverage.out

test-e2e:
	go test ./test/e2e -tags=integration -count=1

test-plugins:
	@echo " > Testing plugins with tag 'plugins'"
	go test ./plugins... -tags=plugins -coverprofile=coverage-plugins.out -parallel=1

test-coverage: # test test-plugins
	cp coverage.out coverage-all.out
	tail -n +2 coverage-plugins.out >> coverage-all.out
	go tool cover -html=coverage-all.out

generate-proto: ## regenerate protos
	@echo " > cloning protobuf from odpf/proton"
	@echo " > generating protobuf"
	@buf generate --template buf.gen.yaml https://github.com/odpf/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --path odpf/assets/v1beta2
	@echo " > protobuf compilation finished"

lint: ## Lint with golangci-lint
	golangci-lint run

install: ## install required dependencies
	@echo "> installing dependencies"
	go install github.com/vektra/mockery/v2@v2.14.0
