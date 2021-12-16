#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
cd "${BASEDIR}" || exit 1

cargo fmt --all
cargo clippy --all --all-targets -- -D clippy::all
cargo clippy --features failpoints --all --all-targets -- -D clippy::all

# to add more ?