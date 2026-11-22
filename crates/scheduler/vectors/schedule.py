"""The scheduler by definition (ADR-0047, K1b), to check the machine against.

Reads JSON lines on stdin, each {name, log, read_ahead, reads, script,
journal}: the command log's text; how far ahead the machine read; when the
reader gave it each line (the environment's, like the script); for every (line,
attempt) the script's sent_after, answer_after (or null) and judgement; and the
machine's journal, an entry per line. Decides every line again from the
definitions, in log order — a line's dependencies are earlier lines, decided
before it — with no window, cells or prefixes, and compares. Exits non-zero on
the first difference.

The definitions:
- read: when the reader gave the line, at max(at - read_ahead, 0) or later.
- binding: a step dependency binds the latest earlier line with that step id
  whose `at` is within W behind the read cursor — the running maximum of `at`
  up to the line — or whose label is retained.
- met: a dependency awaited `sent` is met at its first attempt's sent instant,
  or at its end if it was never sent; `settled` at its end; `expected` at its
  end if it ended expected, else never — the line is skipped at that end. A
  barrier's members are the earlier lines of its kind, scope, label and
  occurrence: met at the latest of theirs; skipped at the first failed
  member's end, for `expected`.
- ready: the latest met instant (0 with no dependency); intended:
  max(at, ready + think); released: max(intended, read).
- deadline: a call's released plus its operation's, channel's or host call's
  timeout; an observation's its cause's intended plus its timeout.
- a captured hole takes its producer's value from the producer's expected
  answer; missing, the line is skipped at its release.
- a line released at or past its deadline ends unknown then, never sent.
- attempts: dispatched at released; sent at dispatch + sent_after if before the
  deadline; answered at sent + answer_after if sent and by the deadline, else
  unknown at the deadline. An expected answer ends it; a not-yet answer of a polled
  observation dispatches the next poll on the grid released + k * every, k at
  least the attempt and the answer's own grid index, ending unknown at the
  deadline if that is not before it; a not-taken answer is resent unless the
  operation is never resent; an unknown is resent only if it is idempotent; any
  other ends it.
- critical: the dependency whose met instant is the latest; compared only when
  one alone attains it, and not for a skipped line.
"""

import json
import sys


def key(step):
    s = step["subject"]
    return (s["kind"], s["scope"], int(s["index"]), step["label"], step["n"])


def holes(step):
    out = []

    def value(v):
        if isinstance(v, dict) and "hole" in v:
            out.append(v["hole"])

    def payload(p):
        if p is not None:
            out.extend(s["hole"] for s in p["holes"])

    def rest(r):
        for v in r["path"]:
            value(v)
        for f in r["query"] + r["headers"]:
            value(f["value"])
        payload(r["body"])

    a = step["action"]
    if "rest" in a:
        rest(a["rest"])
    if "bus" in a:
        value(a["bus"]["key"])
        for f in a["bus"]["headers"]:
            value(f["value"])
        payload(a["bus"]["payload"])
    if "observe" in a:
        o = a["observe"]
        if "rest" in o["probe"]:
            rest(o["probe"]["rest"])
        else:
            value(o["probe"]["bus"]["key"])
        for p in o["until"]:
            t = p["test"]
            for k in ("equals", "contains"):
                if k in t:
                    value(t[k])
    if "host" in a:
        payload(a["host"]["args"])
    return out


COVERED = ("binding_at_release_differs", "capture_after_its_producer_left", "barrier_members_out_of_order",
           "conditional", "answer_after_deadline", "same_instant", "think", "read_after_intended",
           "released_past_deadline", "member_failed_past_the_prefix")


def schedule(text, read_ahead, reads, script, seen):
    lines = text.split("\n")[:-1]
    header = json.loads(lines[0])["header"]
    steps = [json.loads(line)["step"] for line in lines[1:-1]]
    w, retained = int(header["window"]), header["retained"]
    ops = {o["name"]: o for o in header["operations"]}
    channels = {c["name"]: c for c in header["channels"]}
    calls = {c["name"]: c for c in header["calls"]}
    done = {}
    for i, r in enumerate(steps):
        line, at = i + 2, int(r["at"])
        cursor = max(int(e["at"]) for e in steps[: i + 1])
        read = int(reads[str(line)])
        if read < max(at - read_ahead, 0):
            raise SystemExit(f"line {line}: read at {read}, before it may be")

        def bound(step_id):
            for j in range(i - 1, -1, -1):
                e = steps[j]
                if e["step"] == step_id:
                    if e["step"]["label"] in retained or cursor - int(e["at"]) <= w:
                        return j + 2
                    return None
            return None

        def met(dep, on):
            d = done[dep]
            if on == "sent":
                return ("yes", d["sent"] if d["sent"] is not None else d["ended"])
            if on == "settled":
                return ("yes", d["ended"])
            return ("yes", d["ended"]) if d["end"] == "expected" else ("never", d["ended"])

        times, skip, bound_lines = [], None, []
        for dep in r["after"]:
            if "step" in dep:
                b = bound(dep["step"]["step"])
                bound_lines.append(b)
                kind, t = met(b, dep["step"]["on"])
                if kind == "yes":
                    times.append((t, ("step", b)))
                elif skip is None or t < skip[1]:
                    skip = (("dependency", b), t)
            else:
                e = dep["each"]
                members = [
                    j + 2 for j in range(i)
                    if (steps[j]["step"]["subject"]["kind"], steps[j]["step"]["subject"]["scope"],
                        steps[j]["step"]["label"], steps[j]["step"]["n"]) == (e["kind"], e["scope"], e["label"], e["n"])
                ]
                failed = [m for m in members if met(m, e["on"])[0] == "never"]
                if failed:
                    # The member that failed first in time, the lowest on a tie.
                    first = min(failed, key=lambda m: (done[m]["ended"], m))
                    # A member before the failed one still running when it
                    # failed: the holder fails all the same.
                    if any(done[m]["ended"] > done[first]["ended"] for m in members if m < first) or first != failed[0]:
                        seen["member_failed_past_the_prefix"] += 1
                    t = done[first]["ended"]
                    if skip is None or t < skip[1]:
                        skip = (("member", first), t)
                else:
                    for m in members:
                        times.append((met(m, e["on"])[1], ("member", m)))
        ready = max((t for t, _ in times), default=0)
        latest = [c for t, c in times if t == ready]
        critical = latest[0] if len(latest) == 1 else "tie"
        if skip is not None:
            t = max(skip[1], read)
            done[line] = dict(end=("skipped", skip[0]), read=read, intended=t, released=t, sent=None,
                              dispatched=None, ended=t, attempts=0, bound=bound_lines,
                              critical=critical, ready=ready if times else 0, captures=[])
            continue
        intended = max(at, ready + int(r["think"]))
        released = max(intended, read)
        # What the corpus reaches, counted (ADR-0047 K1-6).
        if int(r["think"]) > 0:
            seen["think"] += 1
        if read > intended:
            seen["read_after_intended"] += 1
        # The cursor when the line is released: every line read by then.
        reached = max(int(e["at"]) for j, e in enumerate(steps) if int(reads[str(j + 2)]) <= released)
        for dep in r["after"]:
            if "step" in dep:
                b = bound(dep["step"]["step"])
                d = steps[b - 2]
                if d["step"]["label"] not in retained and reached - int(d["at"]) > w:
                    seen["binding_at_release_differs"] += 1
                if dep["step"]["on"] in ("sent", "settled") and (
                        done[b]["end"] == "unknown" or isinstance(done[b]["end"], tuple)):
                    seen["conditional"] += 1
            else:
                e = dep["each"]
                ends = [done[j + 2]["ended"] for j in range(i)
                        if (steps[j]["step"]["subject"]["kind"], steps[j]["step"]["subject"]["scope"],
                            steps[j]["step"]["label"], steps[j]["step"]["n"]) == (e["kind"], e["scope"], e["label"], e["n"])]
                if ends != sorted(ends):
                    seen["barrier_members_out_of_order"] += 1
        a = r["action"]
        poll = "observe" in a and "rest" in a["observe"]["probe"]
        if "rest" in a:
            deadline, safety = released + int(ops[a["rest"]["op"]]["timeout"]), ops[a["rest"]["op"]]["safety"]
        elif "bus" in a:
            deadline, safety = released + int(channels[a["bus"]["channel"]]["timeout"]), "idempotent"
        elif "host" in a:
            deadline, safety = released + int(calls[a["host"]["op"]]["timeout"]), calls[a["host"]["op"]]["safety"]
        else:
            o = a["observe"]
            cause = line if o["cause"] == r["step"] else bound(o["cause"])
            start = intended if cause == line else done[cause]["intended"]
            deadline, safety = start + int(o["timeout"]), "never"
        entry = dict(read=read, intended=intended, released=released, sent=None, dispatched=None,
                     attempts=0, bound=bound_lines, critical=critical if times else None,
                     ready=ready if times else 0, captures=[])
        # Captured holes take their producers' values.
        missing = None
        for h in holes(r):
            if "captured" in h:
                c = h["captured"]
                producer = bound(c["from"])
                p = steps[producer - 2]
                if p["step"]["label"] not in retained and reached - int(p["at"]) > w:
                    seen["capture_after_its_producer_left"] += 1
                have = [x for x in done.get(producer, {}).get("captures", [])
                        if x["subject"] == c["subject"] and x["slot"] == c["slot"]]
                if not have and missing is None:
                    missing = producer
        if missing is not None:
            done[line] = dict(entry, end=("skipped", ("missing_capture", missing)), ended=released)
            continue
        if released >= deadline:
            # Its window closed before it could start: unknown, never sent.
            seen["released_past_deadline"] += 1
            done[line] = dict(entry, end="unknown", ended=released)
            continue
        t, attempt, end = released, 1, None
        while end is None:
            entry["attempts"] = attempt
            if entry["dispatched"] is None:
                entry["dispatched"] = t
            s = script[f"{line}/{attempt}"]
            sent = t + s["sent_after"]
            if sent < deadline and entry["sent"] is None:
                entry["sent"] = sent
            # An attempt whose bytes never left is answered by nobody.
            answered = None if s["answer_after"] is None or sent >= deadline else sent + s["answer_after"]
            if answered is None or answered > deadline:
                judgement, at_ = "unknown", deadline
                seen["answer_after_deadline"] += 1
            else:
                judgement, at_ = s["judgement"], answered
            if judgement == "expected":
                end, ended = "expected", at_
                entry["captures"] = s.get("captures", [])
            elif judgement in ("unexpected", "not_taken") and poll:
                every = max(int(a["observe"]["every"]), 1)
                k = max(-(-(at_ - released) // every), attempt)
                nxt = released + k * every
                if nxt >= deadline:
                    end, ended = "unknown", deadline
                else:
                    t, attempt = nxt, attempt + 1
            elif judgement == "not_taken" and safety != "never" and at_ < deadline:
                t, attempt = at_, attempt + 1
            elif judgement == "unknown" and safety == "idempotent" and not poll and at_ < deadline:
                t, attempt = at_, attempt + 1
            elif judgement in ("unexpected", "not_taken"):
                end, ended = "unexpected", at_
            else:
                end, ended = "unknown", at_
        done[line] = dict(entry, end=end, ended=ended)
    instants = [d["dispatched"] for d in done.values() if d["dispatched"] is not None]
    if len(instants) != len(set(instants)):
        seen["same_instant"] += 1
    return done


def compare(name, done, journal):
    for e in journal:
        mine = done[e["line"]]
        for f in ("end", "read", "intended", "released", "dispatched", "sent", "ended", "attempts", "bound"):
            theirs = e[f]
            ours = mine[f]
            if f == "end" and isinstance(ours, tuple):
                ours = [ours[0], [ours[1][0], ours[1][1]]]
            if ours != theirs:
                raise SystemExit(f"{name} line {e['line']}: {f} {ours} by definition, {theirs} by the machine")
        skipped = isinstance(mine["end"], tuple)
        if not skipped and mine["critical"] not in ("tie", None) and e["critical"] != [mine["critical"][0], mine["critical"][1]]:
            raise SystemExit(f"{name} line {e['line']}: critical {mine['critical']} by definition, {e['critical']} by the machine")


def main():
    count, seen = 0, {k: 0 for k in COVERED}
    for entry in sys.stdin:
        entry = json.loads(entry)
        done = schedule(entry["log"], entry["read_ahead"], entry["reads"], entry["script"], seen)
        compare(entry["name"], done, entry["journal"])
        count += 1
    if count == 0:
        raise SystemExit("nothing on stdin")
    missed = [k for k, n in seen.items() if n == 0]
    if missed:
        raise SystemExit(f"the corpus does not reach {missed}: the differential does not execute them")
    print(f"{count} runs agree; reached {seen}")


if __name__ == "__main__":
    main()
