.PHONY: test bench lint coverage clean all help check ci install-tools install-hooks

# Default target
all: test lint

# Display help
help:
	@echo "edamame Development Commands"
	@echo "============================"
	@echo ""
	@echo "Testing & Quality:"
	@echo "  make test            - Run unit tests with race detector"
	@echo "  make test-integration- Run integration tests"
	@echo "  make test-bench      - Run performance benchmarks"
	@echo "  make test-all        - Run all tests (unit + integration)"
	@echo "  make bench           - Run benchmarks (legacy alias)"
	@echo "  make lint            - Run linters"
	@echo "  make lint-fix        - Run linters with auto-fix"
	@echo "  make coverage        - Generate coverage report (HTML)"
	@echo "  make check           - Run tests and lint (quick check)"
	@echo "  make ci              - Full CI simulation (all tests + lint + coverage)"
	@echo ""
	@echo "Setup:"
	@echo "  make install-tools   - Install required development tools"
	@echo "  make install-hooks   - Install git pre-commit hook"
	@echo ""
	@echo "Other:"
	@echo "  make clean           - Clean generated files"
	@echo "  make all             - Run tests and lint (default)"

# Run tests with race detector
test:
	@echo "Running tests..."
	@go test -v -race ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=1s ./...

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run --config=.golangci.yml --timeout=5m

# Run linters with auto-fix
lint-fix:
	@echo "Running linters with auto-fix..."
	@golangci-lint run --config=.golangci.yml --fix

# Generate coverage report
coverage:
	@echo "Generating coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1
	@echo "Coverage report generated: coverage.html"

# Clean generated files
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@find . -name "*.test" -delete
	@find . -name "*.prof" -delete
	@find . -name "*.out" -delete

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

# Install git pre-commit hook
install-hooks:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh' > .git/hooks/pre-commit
	@echo 'make check' >> .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed"

# Quick check - run tests and lint
check: test lint
	@echo "All checks passed!"

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	@go test -v -tags=integration ./testing/integration/...

# Run benchmarks
test-bench:
	@echo "Running benchmarks..."
	@go test -v -bench=. -benchmem ./testing/benchmarks/...

# Run all tests (unit + integration)
test-all: test test-integration
	@echo "All tests passed!"

# CI simulation - what CI runs locally
ci: clean lint test test-integration coverage
	@echo "Full CI simulation complete!"
