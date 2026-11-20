# ascetic-ddd-command-log

The planned steps of a load, in the order of their planned time (ADR-0046): what
a generation writes and a load run replays.

**Status: in progress.** Done: the record model and the canonical byte form
(slice C1). To come: templates and holes, the validator, the generation engine,
the offered-load rules.

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
