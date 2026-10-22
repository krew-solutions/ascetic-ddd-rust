# The gates of this workspace, in one list: a developer runs a tier of it,
# CI runs `all`, and nothing else holds the commands. The shape is the one
# of the architecture repository's Makefile, with the tiers this workspace
# has.
#
#   make check       offline, seconds: rustfmt in check mode
#   make clippy      clippy over every crate, feature and target, warnings denied
#   make doc         rustdoc of the workspace, warnings denied: a broken link fails
#   make test        the workspace tests that run as `cargo test` runs them
#   make test-pg     the tests ignored for want of PostgreSQL, run against one
#   make all         everything CI runs on a push: check, clippy, doc, test, test-pg
#   make test-vault  the tests ignored for want of a Vault dev server
#   make test-kafka  the tests ignored for want of Kafka
#   make stand       every tier a full stand serves: all, test-vault, test-kafka
#   make proofs      the TLA+ models under verify/tla, model-checked with TLC
#   make fmt         rustfmt, writing
#   make help        this list
#
# A live test is ignored with its reason, `#[ignore = "needs a live
# PostgreSQL"]`, and the tiers below find their tests by that reason: the
# tests are the list, and this file copies none. The tests of the
# specification crate run against PostgreSQL without being ignored, so
# `test` needs a database as well.
#
# Knobs, taken from the environment or the command line:
#   ASCETIC_DDD_TEST_PG_URL   where the live tests find PostgreSQL; unset, each
#                             crate's tests fall back to a developer's local
#                             database, named in the tests themselves
#   ASCETIC_DDD_TEST_VAULT_ADDR, ASCETIC_DDD_TEST_VAULT_TOKEN
#                             the Vault dev server of `test-vault`
#   ASCETIC_DDD_TEST_KAFKA_BROKERS
#                             the brokers of `test-kafka`
#   TLA2TOOLS                 the jar verify/tla/check.sh runs TLC from
#   CARGO                     the cargo to run (`cargo +nightly`, say)
#
# The experiments under verify/pg are evidence behind ADRs, run by hand
# against a database of one's own; they are no gate.

SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.ONESHELL:
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules --no-builtin-variables
# The tiers share cargo's build lock and one database, and the live tests
# of the inbox assert on time: two of them fail when the machine is busy
# with something else, TLC say. One tier at a time, whatever `-j` asks.
.NOTPARALLEL:

CARGO ?= cargo

# The reasons the tests give for being ignored, as written in them.
NEEDS_PG := needs a live PostgreSQL|requires PostgreSQL
NEEDS_VAULT := needs a Vault dev server
NEEDS_KAFKA := requires Kafka

# Runs the ignored tests of every integration test file that gives the
# reason $(1): the crate is the directory, `crates/<name>` being the package
# `ascetic-ddd-<name>`, and the test target is the file.
define run_ignored
grep -lE '#\[ignore = "($(1))' crates/*/tests/*.rs | while IFS=/ read -r _ crate _ file; do
	$(CARGO) test -p "ascetic-ddd-$$crate" --all-features --test "$${file%.rs}" -- --ignored
done
endef

.PHONY: all stand check clippy doc test test-pg test-vault test-kafka proofs fmt help

all: check clippy doc test test-pg ## everything CI runs on a push: a PostgreSQL beside it

stand: all test-vault test-kafka ## every tier a full stand serves: PostgreSQL, Vault and Kafka

check: ## offline, seconds: rustfmt in check mode
	$(CARGO) fmt --all -- --check

fmt: ## rustfmt, writing
	$(CARGO) fmt --all

clippy: ## clippy over every crate, feature and target, warnings denied
	$(CARGO) clippy --workspace --all-features --all-targets -- -D warnings

doc: ## rustdoc of the workspace, warnings denied: a broken link fails
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --all-features --no-deps

test: ## the workspace tests as `cargo test` runs them; the specification crate's need PostgreSQL
	$(CARGO) test --workspace --all-features

test-pg: ## the tests ignored for want of PostgreSQL, at ASCETIC_DDD_TEST_PG_URL
	$(call run_ignored,$(NEEDS_PG))

test-vault: ## the tests ignored for want of a Vault dev server, at ASCETIC_DDD_TEST_VAULT_ADDR
	$(call run_ignored,$(NEEDS_VAULT))

test-kafka: ## the tests ignored for want of Kafka, at ASCETIC_DDD_TEST_KAFKA_BROKERS
	$(call run_ignored,$(NEEDS_KAFKA))

proofs: ## the TLA+ models under verify/tla, model-checked with TLC (java, tla2tools.jar)
	verify/tla/check.sh

help: ## this list
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
