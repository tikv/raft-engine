# Makefile

## Additionaly arguments passed to cargo.
EXTRA_CARGO_ARGS ?=
## How to test stable toolchain.
## - auto: use current default toolchain, disable nightly features.
## - force: always use stable toolchain, disable nightly features.
WITH_STABLE_TOOLCHAIN ?=

TOOLCHAIN_ARGS =
ifeq ($(shell (rustc --version | grep -q nightly); echo $$?), 1)
ifeq (,$(filter $(WITH_STABLE_TOOLCHAIN),auto force))
# Force use nightly toolchain if we are building with nightly features.
TOOLCHAIN_ARGS = +nightly
endif
else
ifeq ($(WITH_STABLE_TOOLCHAIN), force)
TOOLCHAIN_ARGS = +stable
endif
endif

.PHONY: format clippy test

all: format clippy test

## Format code in-place using rustfmt.
format:
	cargo fmt --all

## Run clippy.
clippy:
ifneq (,$(filter $(WITH_STABLE_TOOLCHAIN),auto force))
	cargo ${TOOLCHAIN_ARGS} clippy --all --features all_stable --all-targets -- -D clippy::all
else
	cargo ${TOOLCHAIN_ARGS} clippy --all --all-features --all-targets -- -D clippy::all
endif

## Run tests.
test:
ifneq (,$(filter $(WITH_STABLE_TOOLCHAIN),auto force))
	cargo ${TOOLCHAIN_ARGS} test --all --features all_stable_except_failpoints ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --features all_stable ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
else
	cargo ${TOOLCHAIN_ARGS} test --all --features all_except_failpoints ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --all-features ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
endif
