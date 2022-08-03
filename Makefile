# Makefile

## Additionaly arguments passed to cargo.
EXTRA_CARGO_ARGS ?=
## Whether to disable nightly-only feature. [true/false]
WITH_STABLE_TOOLCHAIN ?=

.PHONY: format clippy test

all: format clippy test

## Format code in-place using rustfmt.
format:
	cargo fmt --all

## Run clippy.
ifeq ($(WITH_STABLE_TOOLCHAIN), true)
clippy:
	cargo clippy --all --features all_stable --all-targets -- -D clippy::all
else
clippy:
	cargo clippy --all --all-features --all-targets -- -D clippy::all
endif

## Run tests.
ifeq ($(WITH_STABLE_TOOLCHAIN), true)
test:
	cargo test --all --features all_stable_except_failpoints ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo test --test failpoints --features all_stable ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
else
test:
	cargo test --all --features all_except_failpoints ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo test --test failpoints --all-features ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
endif
