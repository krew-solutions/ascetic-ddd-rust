# The gates of this workspace, in one list: a developer runs a tier of it,
# CI runs `all` on every push and `proofs` and `traces` in a pipeline of
# their own (.github/workflows), and nothing else holds the commands. The shape is the one
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
#   make proofs-groups  the groups `proofs` falls into, for a job each
#   make traces      the traces recorded again by the live tests, then checked
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
#   PROOFS                    the groups of verify/tla/check.sh `proofs` runs;
#                             unset, every group
#   CARGO                     the cargo to run (`cargo +nightly`, say)
#
# The experiments under verify/pg are evidence behind ADRs, run by hand
# against a database of one's own; they are no gate.

SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.ONESHELL:
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules --no-builtin-variables
# The tiers share cargo's build lock and one database, and TLC takes every
# core. One tier at a time, whatever `-j` asks.
.NOTPARALLEL:

CARGO ?= cargo

# The reasons the tests give for being ignored, as written in them.
NEEDS_PG := needs a live PostgreSQL|requires PostgreSQL
NEEDS_VAULT := needs a Vault dev server
NEEDS_KAFKA := requires Kafka

# Runs the ignored tests of every integration test file that gives the
# reason $(1): the crate is the directory, `crates/<name>` being the package
# `ascetic-ddd-<name>`, and the test target is the file. Then those of every
# library whose sources give it, as its `--lib` target: a test that reads
# the crate's private statements lives beside them (`plan_tests.rs`). A grep
# that finds nothing is no failure; one that cannot read is.
define run_ignored
{ grep -lE '#\[ignore = "($(1))' crates/*/tests/*.rs || test $$? -eq 1; } | while IFS=/ read -r _ crate _ file; do
	$(CARGO) test -p "ascetic-ddd-$$crate" --all-features --test "$${file%.rs}" -- --ignored
done
{ grep -rlE '#\[ignore = "($(1))' crates/*/src || test $$? -eq 1; } | cut -d/ -f2 | sort -u | while read -r crate; do
	$(CARGO) test -p "ascetic-ddd-$$crate" --all-features --lib -- --ignored
done
endef

.PHONY: all stand check clippy doc test test-pg host-features vectors test-vault test-kafka proofs proofs-groups traces fmt help

all: check clippy doc test test-pg host-features vectors ## everything CI runs on a push: a PostgreSQL beside it

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

# The library built as a host builds it (ADR-0037 § Executors): a workspace
# of its own, since this one tests with `--all-features`.
HOST_FEATURES := --manifest-path verify/inbox-features/Cargo.toml

host-features: ## the stream id's golden table and properties, the command log's golden log and the scheduler's golden journal, under serde_json's arbitrary_precision and preserve_order, then raw_value too (verify/inbox-features)
	$(CARGO) fmt $(HOST_FEATURES) -- --check
	$(CARGO) clippy $(HOST_FEATURES) --all-targets --all-features -- -D warnings
	$(CARGO) test $(HOST_FEATURES)
	$(CARGO) test $(HOST_FEATURES) --features raw_value

vectors: ## byte forms checked outside the crates, python3's standard library only: faker's SipHash against the reference implementation's published vector, the command log's golden log and the scheduler's golden journal and golden runs against their normative schemas and their trailers, its renderings against golden/render.txt, its compressed logs through the zstd CLI
	python3 -I crates/faker/vectors/key_vectors.py > /dev/null
	python3 -I crates/command-log/vectors/format.py crates/command-log/golden/c1.jsonl
	python3 -I crates/scheduler/vectors/journal.py crates/scheduler/golden/journal.jsonl
	for f in crates/scheduler/golden/runs/*.jsonl; do python3 -I crates/scheduler/vectors/journal.py $$f > /dev/null || exit 1; done
	python3 -I crates/command-log/vectors/render.py | diff - crates/command-log/golden/render.txt
	$(CARGO) run -q -p ascetic-ddd-command-log --example zstd_golden | zstd -dc | cmp - crates/command-log/golden/c1.jsonl
	zstd -dc crates/command-log/golden/c1.jsonl.zst | cmp - crates/command-log/golden/c1.jsonl

test-vault: ## the tests ignored for want of a Vault dev server, at ASCETIC_DDD_TEST_VAULT_ADDR
	$(call run_ignored,$(NEEDS_VAULT))

test-kafka: ## the tests ignored for want of Kafka, at ASCETIC_DDD_TEST_KAFKA_BROKERS
	$(call run_ignored,$(NEEDS_KAFKA))

PROOFS ?=

proofs: ## the TLA+ models under verify/tla, model-checked with TLC (java, tla2tools.jar); PROOFS='<group> ...' for some
	verify/tla/check.sh $(PROOFS)

proofs-groups: ## the groups `proofs` falls into, a name a line: a pipeline runs each as a job of its own
	@verify/tla/check.sh --groups

# The live tests that record a trace, run with the recorder on, then the
# trace checks alone: a trace replayed but never recorded again checks the
# code as it was (ADR-0041 decision 8).
# - The traces the repository holds are the covered ones, and the list:
#   each is replaced by its new recording, and one that no test recorded
#   again fails the target, its test gone or renamed.
# - A recording the repository does not hold is named, not checked: the
#   models do not express what its test runs (verify/tla/README.md, Trace
#   validation). Committing it, once it fits, covers it.
# - The directory is absolute: cargo runs a test in its crate's directory,
#   where a relative one would land.
# - One test at a time: a sibling's open transaction would stand in a
#   recorded snapshot, a step no trace records (measured: the bridge's trace
#   then fits or not as the siblings happen to run).
traces: ## the traces recorded again by the live tests at ASCETIC_DDD_TEST_PG_URL, then checked against the models
	shopt -s nullglob
	recorded=$$(mktemp -d)
	trap 'rm -rf "$$recorded"' EXIT
	ASCETIC_DDD_TRACE_DIR="$$recorded" $(CARGO) test -p ascetic-ddd-outbox -p ascetic-ddd-inbox \
	  --all-features --test pg --test bridge --test outage -- --include-ignored --test-threads=1
	ASCETIC_DDD_TRACE_DIR="$$recorded" $(CARGO) test -p ascetic-ddd-scheduler --test trace
	gone=0
	for kept in verify/tla/traces/*.jsonl; do
	  if [ -f "$$recorded/$${kept##*/}" ]; then cp "$$recorded/$${kept##*/}" "$$kept"
	  else echo "traces: $${kept##*/} is covered, and no test recorded it again" >&2; gone=1; fi
	done
	verify/tla/check.sh --uncovered "$$recorded" || gone=1
	[ "$$gone" -eq 0 ]
	verify/tla/check.sh traces

help: ## this list
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'
