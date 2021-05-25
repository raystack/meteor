NAME="github.com/odpf/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"

.PHONY: all build clean

all: build

build:
	go build -ldflags "-X main.Version=${VERSION}" ${NAME}

clean:
	rm -rf dist/

test:
	go test ./... -coverprofile=coverage.out

test-coverage: test
	go tool cover -html=coverage.out

dist:
	@bash ./scripts/build.sh

check-swagger:
	which swagger || (GO111MODULE=off go get -u github.com/go-swagger/go-swagger/cmd/swagger)

swagger: check-swagger
	GO111MODULE=on go mod vendor  && GO111MODULE=off swagger generate spec -o ./swagger/swagger.yaml --scan-models

swagger-serve: check-swagger
	swagger serve -F=swagger api/handlers/swagger.yaml
