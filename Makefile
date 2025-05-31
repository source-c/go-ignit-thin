.PHONY: generate build lint test test-ci clean

PACKAGES = $(shell go list ./... | grep -v benchmarks)
TEST_FLAGS ?= -v
GOPATH=$(shell go env GOPATH)
BENCH="."
WARMUPS=3
IGNITE_HOSTS=localhost:10800
IGNITE_START_TIMEOUT=30
JUNIT_REPORTER_FLAGS=""

build:
	go build $(PACKAGES)

generate:
	go get .
	go generate $(PACKAGES)

lint:
	go install honnef.co/go/tools/cmd/staticcheck@v0.4.7;
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.52.2;
	$(GOPATH)/bin/staticcheck --tags=testing ./...
	$(GOPATH)/bin/golangci-lint run --build-tags testing ./...
	go vet --tags=testing ./...

kill-ignite:
	jps -v | grep ignite | cut -d' ' -f1 | xargs -r kill -9

test: build kill-ignite
	@echo "IGNITE_HOME="$(IGNITE_HOME)
	IGNITE_START_TIMEOUT=$(IGNITE_START_TIMEOUT) go test -race --tags=testing $(TEST_FLAGS) $(PACKAGES)

test-ci: build kill-ignite
	go install github.com/jstemmer/go-junit-report/v2@v2.1.0
	IGNITE_START_TIMEOUT=$(IGNITE_START_TIMEOUT) go test -race --tags=testing -coverprofile=coverage.out $(TEST_FLAGS) $(PACKAGES)  2>&1 | $(GOPATH)/bin/go-junit-report $(JUNIT_REPORTER_FLAGS) > test-report.xml

bench: build
	IGNITE_START_TIMEOUT=$(IGNITE_START_TIMEOUT) WARMUPS=$(WARMUPS) IGNITE_HOSTS=$(IGNITE_HOSTS) go test -bench=$(BENCH) -test.benchtime=10s -timeout=40m --tags=testing $(TEST_FLAGS) ./benchmarks

clean:
	find . -name 'ignite-config-*.xml' -delete
	find . -name 'log4j-*.xml' -delete
	find . -name 'ignite-log-*.txt' -delete
	find . -name 'test-report.xml' -delete
	find . -name 'coverage.out' -delete

