.PHONY: build lint fmt check clean test

build:
	cargo build

lint:
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt --all -- --check

check: lint fmt
	cargo check --all-targets --all-features

test:
	cargo test

clean:
	cargo clean
