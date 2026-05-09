.PHONY: build test lint fmt

build:
	go build ./...

test:
	go test -race ./...

lint:
	@echo "==> gofmt"
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "gofmt issues:"; echo "$$out"; exit 1; fi
	@echo "==> go vet"
	go vet ./...
	@echo "==> staticcheck"
	@if ! command -v staticcheck >/dev/null 2>&1 && [ ! -x "$$HOME/go/bin/staticcheck" ]; then \
		echo "staticcheck not installed; run: go install honnef.co/go/tools/cmd/staticcheck@latest"; \
		exit 1; \
	fi
	@if command -v staticcheck >/dev/null 2>&1; then staticcheck ./...; else $$HOME/go/bin/staticcheck ./...; fi

fmt:
	gofmt -w .
