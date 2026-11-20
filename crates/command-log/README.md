# ascetic-ddd-command-log

The planned steps of a load, in the order of their planned time (ADR-0046): what
a generation writes and a load run replays.

**Status: in progress.** Done: the record model and the canonical byte form
(slice C1); templates, the rendering of holes and the splice (C2); the
validator (C3); the generation engine (C4). To come: the offered-load rules.

## A log

A log is JSON Lines: a header, one line a planned step, a trailer.

- **A step** is a command or an observation, identified from identity alone
  (subject, label, occurrence), planned at a time, ordered by its edges: what
  must happen first, a barrier over a wave, the step before it on its stream.
- **A value the generation cannot know is a hole** beside the bytes it fills:
  rendered at replay from the run's namespace and a subject, captured from an
  earlier step's outcome, or a time.
- **One byte form.** Fields in their declared order, no whitespace; integers
  that can pass 2⁵³ as strings; bytes as base64. A reader re-encodes every line
  and refuses one that differs.
- **A trailer** carries the number of steps and the SHA-256 of every line before
  it: a truncated log is refused, never replayed as a shorter run.

## A template

A host serialises its typed message — a DTO from its OpenAPI, a prost message —
as it always does, with a sentinel from `template::Template` in each field a
hole fills: a valid value of that field's type, so the message holds it. The
template finds each sentinel once in the bytes and records its span. At replay,
`render::fill` splices each span's value: an id rendered from the run's
namespace and the subject, a time, a captured value.

## The validator

`validate::validate` reads a log once, in memory bounded by the window, and
reports every rule a line breaks: order, unique step ids, edges within the
window, barrier counts, the order of a captured slot's writers and readers,
streams and their gaps, spans, ids against the stand's profile, operations,
deviations. What only the host knows — whether a span is its field, which
dependencies its domain requires — it asks through `validate::Host`. An
independent reference, `vectors/reference.py`, decides the same rules from their
definitions and agrees with it on a corpus of random logs and their mutants.

## The engine

`engine::generate` runs units of identity — a population family, a scope, an
index range — on any number of threads. Each unit emits its steps in any order;
the engine buckets them by time cell, spills sorted runs past a byte budget,
and merges each cell by `(at, StepId)` into the log. Neither the threads, the
budget, the cells nor the units' order changes a byte; two steps with one
`(at, StepId)` fail the generation.
