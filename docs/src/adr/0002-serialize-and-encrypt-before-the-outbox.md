# ADR-0002: Where a message is serialized and encrypted relative to the outbox

## Status

Accepted (2026-09-09): option 1. `payload` is `BYTEA`, `metadata` stays JSONB.

## Context

The target system is a product under strict regulation: messages
carry device and user data and must be encrypted at rest. Integration events
cross the process boundary as Protocol Buffers on Kafka (CLAUDE.md, contract
table). The Python `ascetic_ddd.outbox` stores `payload` as JSONB and leaves
serialization for the broker to the dispatcher; its design notes chose JSON so
that one outbox could feed brokers of different formats, stay queryable in
`psql`, and serve in-process consumers with the event itself. Encryption was
not part of that reasoning.

The bus crate already separates a typed layer (`Producer<T>` with an encoder,
`Consumer<T>` with a decoder) from a wire layer (`WireProducer`,
`WireConsumer`, `Message` = bytes + key). The question is which side of the
outbox the typed layer sits on.

## Option 1: serialize and encrypt before the outbox

The producer's encoder turns the event into wire bytes (proto) and encrypts
them; the outbox stores ciphertext in `BYTEA` and metadata in JSONB; the
dispatcher relays bytes. The outbox is a wire-level stage of the bus.

For:
- Plaintext never reaches the database. Encryption at the origin is the
  strongest at-rest guarantee and the easiest to audit.
- One serialization, at the one place where the type is known. No JSON round
  trip, no lossy re-typing (decimals, enums, versions).
- The dispatcher knows no schemas and depends on no domain crate: bytes in,
  bytes out, key from the URI, headers from metadata.
- Fits the bus as it is: the outbox becomes a `WireProducer` stage
  (`outbox://kafka/…`); it needs the session at the wire layer, which is the
  subject of the next ADR.

Against, and the attempt to refute each:
- The outbox is no longer readable in `psql`. — Under encryption at rest that
  is the goal, not a loss; metadata stays in clear for routing and support.
- A row is bound to one wire contract and one key. — A URI names the
  transport and the channel, and one channel has one contract; the bus
  adapter does no routing. An event that must reach a second audience in
  another format or under another key is published twice, to two URIs, by
  the publishing adapter at the origin, where the type is known. Nothing
  is transcoded downstream.
- The command's transaction pays for encryption. — Symmetric encryption of a
  few kilobytes is microseconds; the key-encryption key is fetched once per
  key version. A KMS outage fails the command, which is failing closed.
- Metadata in clear may still leak identifiers (tenant, stream ids in causal
  dependencies). — Open: decide what metadata may carry in clear.

## Option 2: serialize and encrypt after the outbox

The outbox stores the event as JSONB, as the Python source does; the
dispatcher deserializes, re-serializes to proto, encrypts, and sends.

For:
- Queryable, debuggable outbox; format-agnostic rows; in-process consumers
  read the event directly. The reasoning of the design notes.

Against:
- Plaintext at rest in the database, unless the database itself is
  encrypted — weaker and harder to audit than per-message encryption.
- Two serializations per message, and the JSON step loses types; the
  dispatcher must know every event type and schema, which couples one
  process to the contracts of every bounded context.
- Encryption in the dispatcher means the key material lives in the process
  with the least domain knowledge and the most throughput.

## Proposed decision

Option 1. The typed layer of the bus — encoder and decoder — is where an
event is serialized and encrypted; the outbox and the inbox are wire-level
stages that store and relay bytes plus metadata.

## Consequences

- `OutboxMessage.payload` becomes bytes; `metadata` stays JSONB. The inbox
  mirrors this.
- The bus needs no change to accept bytes: its wire layer already does.
  What it lacks is the session for the outbox and inbox stages, and a
  stage for encryption; both are ADR-0003.
- Envelope encryption needs a key id in the metadata; the Python project's
  KMS module and its envelope-encryption ADR are the reference.
- Consumers that want the event in clear decrypt in their decoder; nothing
  in the pipeline sees plaintext but the two ends.

## Open

- What metadata may travel in clear.
- Whether encryption is per message (envelope) or per topic key.
