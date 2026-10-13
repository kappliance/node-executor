GO ?= go
HELPER_IMAGE ?= node-executor:local

.PHONY: test test-api vet build image
test:
	$(GO) test -race ./...

test-api:
	$(GO) test -race -tags=integration ./...

vet:
	$(GO) vet ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -o bin/node-executor ./cmd/node-executor

image: build
	docker build --platform linux/$(shell $(GO) env GOARCH) -t $(HELPER_IMAGE) .
