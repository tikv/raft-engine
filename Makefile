# Makefile

## Additionaly arguments passed to cargo.
EXTRA_CARGO_ARGS ?=
## How to test stable toolchain.
## - auto: use current default toolchain, disable nightly features.
## - force: explicitly use stable toolchain, disable nightly features.
WITH_STABLE_TOOLCHAIN ?=

WITH_NIGHTLY_FEATURES =
ifeq (,$(filter $(WITH_STABLE_TOOLCHAIN),auto force))
WITH_NIGHTLY_FEATURES = 1
endif

TOOLCHAIN_ARGS =
ifeq ($(shell (rustc --version | grep -q nightly); echo $$?), 1)
ifdef WITH_NIGHTLY_FEATURES
# Force use nightly toolchain if we are building with nightly features.
TOOLCHAIN_ARGS = +nightly
endif
else
ifeq ($(WITH_STABLE_TOOLCHAIN), force)
TOOLCHAIN_ARGS = +stable
endif
endif

BIN_PATH = $(CURDIR)/bin
CARGO_TARGET_DIR ?= $(CURDIR)/target/
export RUST_LOG=info

.PHONY: clean format clippy test
.PHONY: ctl

all: format clippy test

clean:
	cargo clean
	rm -rf ${BIN_PATH}

## Format code in-place using rustfmt.
format:
	cargo ${TOOLCHAIN_ARGS} fmt --all

CLIPPY_WHITELIST += -A clippy::bool_assert_comparison
## Run clippy.
clippy:
ifdef WITH_NIGHTLY_FEATURES
	cargo ${TOOLCHAIN_ARGS} clippy --all --features nightly_group,failpoints --all-targets -- -D clippy::all ${CLIPPY_WHITELIST}
else
	cargo ${TOOLCHAIN_ARGS} clippy --all --features failpoints --all-targets -- -D clippy::all ${CLIPPY_WHITELIST}
endif

## Run tests.
test:
ifdef WITH_NIGHTLY_FEATURES
	cargo ${TOOLCHAIN_ARGS} test --all --features nightly_group ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --features nightly_group,failpoints ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
else
	cargo ${TOOLCHAIN_ARGS} test --all ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --features failpoints ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
endif

## Run tests with various features for maximum code coverage.
ifndef WITH_NIGHTLY_FEATURES
test_matrix:
	$(error Must run test matrix with nightly features. Please reset WITH_STABLE_TOOLCHAIN.)
else
test_matrix: test
	cargo ${TOOLCHAIN_ARGS} test --all ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --features failpoints ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
	cargo ${TOOLCHAIN_ARGS} test --all --features nightly_group,std_fs ${EXTRA_CARGO_ARGS} -- --nocapture
	cargo ${TOOLCHAIN_ARGS} test --test failpoints --features nightly_group,std_fs,failpoints ${EXTRA_CARGO_ARGS} -- --test-threads 1 --nocapture
endif

## Build raft-engine-ctl.
ctl:
	cargo build --release --package raft-engine-ctl
	@mkdir -p ${BIN_PATH}
	@cp -f ${CARGO_TARGET_DIR}/release/raft-engine-ctl ${BIN_PATH}/
