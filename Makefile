.PHONY: help build release test smoke lint fmt fmt-check clean install uninstall stats-help run-help
.DEFAULT_GOAL := help

PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*## "} {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build:  ## Debug build
	cargo build

release:  ## Optimised release build (target/release/vatsim-recorder)
	cargo build --release

test:  ## Run unit + integration tests (skips the ignored real-endpoint smoke)
	cargo test

smoke:  ## Run the real-endpoint smoke test against live VATSIM (network required)
	cargo test --test real_smoke -- --ignored --nocapture

lint:  ## Clippy with warnings treated as errors
	cargo clippy --all-targets -- -D warnings

fmt:  ## Apply rustfmt
	cargo fmt

fmt-check:  ## Verify rustfmt compliance without modifying files
	cargo fmt --check

clean:  ## Remove target/ build artefacts
	cargo clean

install: release  ## Install the release binary to $(DESTDIR)$(BINDIR) (default /usr/local/bin; override with PREFIX=)
	install -Dm755 target/release/vatsim-recorder $(DESTDIR)$(BINDIR)/vatsim-recorder
	@echo "Installed to $(DESTDIR)$(BINDIR)/vatsim-recorder"

uninstall:  ## Remove the installed binary from $(DESTDIR)$(BINDIR)
	rm -f $(DESTDIR)$(BINDIR)/vatsim-recorder
	@echo "Removed $(DESTDIR)$(BINDIR)/vatsim-recorder"

run-help:  ## Show `vatsim-recorder run --help`
	cargo run --release --quiet -- run --help

stats-help:  ## Show `vatsim-recorder stats --help`
	cargo run --release --quiet -- stats --help
