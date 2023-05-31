.PHONY: all build clean test
all: build

# HELP sourced from https://gist.github.com/prwhite/8168133

# Add help text after each target name starting with '\#\#'
# A category can be added with @category

HELP_FUNC = \
    %help; \
    while(<>) { \
        if(/^([a-z0-9_-]+):.*\#\#(?:@(\w+))?\s(.*)$$/) { \
            push(@{$$help{$$2}}, [$$1, $$3]); \
        } \
    }; \
    print "usage: make [target]\n\n"; \
    for ( sort keys %help ) { \
        print "$$_:\n"; \
        printf("  %-30s %s\n", $$_->[0], $$_->[1]) for @{$$help{$$_}}; \
        print "\n"; \
    }

help:           ##@help show this help
	@perl -e '$(HELP_FUNC)' $(MAKEFILE_LIST)

NAME="github.com/goto/meteor"
VERSION=$(shell git describe --always --tags 2>/dev/null)
COVERFILE="/tmp/app.coverprofile"
PROTON_COMMIT := "5b5dc727b525925bcec025b355983ca61d7ccf68"

TOOLS_MOD_DIR = ./tools
TOOLS_DIR = $(abspath ./.tools)

define build_tool
$(TOOLS_DIR)/$(1): $(TOOLS_MOD_DIR)/go.mod $(TOOLS_MOD_DIR)/go.sum $(TOOLS_MOD_DIR)/tools.go
	cd $(TOOLS_MOD_DIR) && \
	go build -o $(TOOLS_DIR)/$(1) $(2)
endef

$(eval $(call build_tool,buf,github.com/bufbuild/buf/cmd/buf))
$(eval $(call build_tool,golangci-lint,github.com/golangci/golangci-lint/cmd/golangci-lint))
$(eval $(call build_tool,mockery,github.com/vektra/mockery/v2))
$(eval $(call build_tool,gofumpt,mvdan.cc/gofumpt))
$(eval $(call build_tool,gci,github.com/daixiang0/gci))

# DEV SETUP #############

install: $(TOOLS_DIR)/buf $(TOOLS_DIR)/golangci-lint $(TOOLS_DIR)/mockery $(TOOLS_DIR)/gofumpt $(TOOLS_DIR)/gci
	@echo "All tools installed successfully"

copy-config:
	cp ./config/meteor.yaml.sample ./meteor.yaml

imports: $(TOOLS_DIR)/gci ##@dev_setup does a goimports
	$(TOOLS_DIR)/gci write ./ --section standard --section default --skip-generated

fmt: $(TOOLS_DIR)/gofumpt imports ##@dev_setup does a go fmt (stricter variant)
	$(TOOLS_DIR)/gofumpt -l -w -extra .

lint: $(TOOLS_DIR)/golangci-lint ##@dev_setup lint source
	$(TOOLS_DIR)/golangci-lint --config=".golangci-prod.toml" --new-from-rev=HEAD~1 --max-same-issues=0 --max-issues-per-linter=0 run

# BUILD #############

generate-proto: $(TOOLS_DIR)/buf ## regenerate protos
	@echo " > generating protobuf"
	$(TOOLS_DIR)/buf generate --template buf.gen.yaml https://github.com/goto/proton/archive/${PROTON_COMMIT}.zip#strip_components=1 --path gotocompany/assets/v1beta2
	@echo " > protobuf compilation finished"

build:
	go build -ldflags "-X cmd.Version=${VERSION}" ${NAME}

build-dev:
	CGO_ENABLED=0 go build -ldflags "-X cmd.Version=dev" ${NAME}

clean:
	rm -rf dist/

# TESTS #############

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
