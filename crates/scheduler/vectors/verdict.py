"""The verdict by definition (ADR-0047 decision 14, as decided for K1e-3).

Reads, on stdin, one JSON object a line: {"name", "log": {"sha256", "records"},
"journal": [the journal's lines, parsed], "verdict", "classes"} — the latter two
what the scheduler's fold gave — and computes the verdict and every line's
class from the decision's sentences, in memory, each rule its own function:
no byte a line, no second pass, no barrier flags. Stops at the first
difference. Prints what it reached.

The standard library only. Run by crates/scheduler/tests/verdict.rs and
tests/reference.rs.
"""

import collections
import json
import sys

REACHED = collections.Counter()


class Incoherent(Exception):
    pass


class AnotherLog(Exception):
    pass


def u(v):
    return None if v is None else int(v)


def end_kind(e):
    end = e["end"]
    return "skipped" if isinstance(end, dict) else end


def skip_of(e):
    end = e["end"]
    if not isinstance(end, dict):
        return None
    (why, line), = end["skipped"].items()
    return why, int(line)


class Run:
    def __init__(self, lines, log):
        header = lines[0]["header"]
        if header["log"] != log["sha256"]:
            raise AnotherLog()
        self.j = int(header["jitter"])
        self.floor = int(header["resend"])
        body = lines[1:-1]
        self.reads, self.ends, self.released = {}, {}, {}
        self.events = collections.defaultdict(list)  # line -> [(index, kind, value)]
        self.faults = {}
        for i, l in enumerate(body):
            (kind, v), = l.items()
            line = int(v["line"])
            if kind == "read":
                if line in self.reads:
                    raise Incoherent("read twice")
                self.reads[line] = (i, v)
            elif kind == "end":
                if line in self.ends:
                    raise Incoherent("ended twice")
                self.ends[line] = (i, v)
            elif kind == "fault":
                self.faults[line] = i
            else:
                if kind == "released":
                    if line in self.released:
                        raise Incoherent("released twice")
                    self.released[line] = v
                self.events[line].append((i, kind, v))
        order = sorted(self.reads)
        if [self.reads[l][0] for l in order] != sorted(self.reads[l][0] for l in order):
            raise Incoherent("reads not in line order")
        if order != list(range(2, 2 + len(order))):
            raise Incoherent("reads not one a line")
        if set(self.ends) != set(self.reads):
            raise Incoherent("a read without its end, or an end without its read")
        if set(self.faults) - set(self.reads):
            raise Incoherent("a fault of a line not read")
        self.order = order
        for l in order:
            self.coherent(l)
        # The journal sound, it must be all of its log's run.
        if len(order) != log["records"]:
            raise AnotherLog()

    def read(self, l):
        return self.reads[l][1]

    def end(self, l):
        return self.ends[l][1]

    def coherent(self, l):
        """Every end agrees with its step's events (decision 14: evidence)."""
        ri, r = self.reads[l]
        ei, e = self.ends[l]
        if ei < ri:
            raise Incoherent("ended before read")
        attempts, first_dispatch, sent, taken = 0, None, None, None
        last_dispatch, last_sent = None, None
        answered = None  # the last taken answer: (attempt, at, after)
        rel = None
        for i, kind, v in self.events[l]:
            if i < ri:
                raise Incoherent("an event before its read")
            if kind == "sent" and i > ei:
                continue
            if kind == "answer" and i > ei:
                if v["ignored"] is None:
                    raise Incoherent("an answer taken after its end")
                continue
            if i > ei:
                raise Incoherent("an event after its end")
            if kind == "released":
                if u(v["released"]) != max(u(v["intended"]), u(r["at"])) or u(v["at"]) < u(r["at"]):
                    raise Incoherent("a release against its read")
                rel = v
            elif kind == "dispatch":
                if rel is None or v["attempt"] != attempts + 1 or u(v["at"]) < u(rel["released"]):
                    raise Incoherent("an attempt out of turn")
                if v["attempt"] > 1 and not self.licensed(r, rel, taken, answered, u(v["at"])):
                    raise Incoherent("a resend its answer did not license")
                attempts = v["attempt"]
                first_dispatch = u(v["at"]) if first_dispatch is None else first_dispatch
                last_dispatch, last_sent, taken, answered = u(v["at"]), None, None, None
            elif kind == "sent":
                if v["attempt"] > attempts or v["attempt"] == 0:
                    raise Incoherent("a send of an attempt not dispatched")
                if v["attempt"] == attempts:
                    last_sent = u(v["at"]) if last_sent is None else last_sent
                    sent = u(v["at"]) if sent is None else sent
            elif kind == "answer":
                if v["ignored"] is None:
                    if v["attempt"] != attempts or attempts == 0:
                        raise Incoherent("an answer taken from an attempt not out")
                    taken = v["judged"]
                    answered = (v["attempt"], u(v["at"]), u(v["after"]))
                elif v["attempt"] > attempts:
                    raise Incoherent("an answer of an attempt not dispatched")
        if (
            u(e["read"]) != u(r["at"])
            or e["attempts"] != attempts
            or u(e["dispatched"]) != first_dispatch
            or u(e["sent"]) != sent
        ):
            raise Incoherent("an end against its events")
        if rel is not None:
            if (u(e["intended"]), u(e["released"])) != (u(rel["intended"]), u(rel["released"])):
                raise Incoherent("an end against its release")
        elif e["dispatched"] is not None:
            raise Incoherent("dispatched, never released")
        kind = end_kind(e)
        ok = {
            "expected": taken == "expected" and not e["fault"],
            "unexpected": taken in ("unexpected", "not_taken") and not e["fault"],
            "unknown": e["fault"] or attempts == 0 or taken != "expected",
            "skipped": attempts == 0 and not e["fault"],
        }[kind]
        if not ok:
            raise Incoherent("an end against its answers")
        if e["fault"] != (l in self.faults):
            raise Incoherent("a backstop without its fault, or a fault without it")
        if e["fault"] and self.faults[l] != ei + 1:
            raise Incoherent("a fault not right after its end")
        skip = skip_of(e)
        if skip:
            why, d = skip
            failed = d in self.ends and end_kind(self.end(d)) != "expected" and self.ends[d][0] < ei
            if why == "dependency":
                steps = [(x["step"]["on"], int(x["step"]["line"])) for x in r["after"] if "step" in x]
                if ("expected", d) not in steps or not failed:
                    raise Incoherent("skipped for a dependency that did not fail it")
            elif why == "member":
                if not (2 <= d < l) or not failed:
                    raise Incoherent("skipped for a member that did not fail it")
            elif not (2 <= d < l):
                raise Incoherent("skipped for a producer not read before it")
        self.taken_attempts = attempts

    def licensed(self, r, rel, taken, answered, at):
        """Decision 15: a poll's next after an answer not expected, no sooner
        than its wait; a call's resend after a certified refusal of an operation
        that may be resent, or an unknown answer of an idempotent one, at the
        later of its wait and the floor, more than J before its deadline."""
        if answered is None:
            return False
        _, then, after = answered
        wait = after or 0
        if r["action"] == "poll":
            return taken in ("unexpected", "not_taken", "unknown") and at >= then + wait
        resendable = (taken == "not_taken" and r["safety"] != "never") or (
            taken == "unknown" and r["safety"] == "idempotent")
        return resendable and at >= then + max(wait, self.floor) and at + self.j < u(rel["deadline"])

    # The decision's sentences.

    def members(self, holder, key, count):
        """A barrier's members: its key's lines read before the holder (decision 8)."""
        ms = [m for m in self.order if m < holder and self.read(m)["key"] == key]
        if len(ms) != count:
            raise Incoherent("a barrier's members miscounted")
        return ms

    def awaited(self, l):
        """(strength, line) of every step the line awaits."""
        out = []
        for d in self.read(l)["after"]:
            if "step" in d:
                dep = int(d["step"]["line"])
                if not (2 <= dep < l):
                    raise Incoherent("a dependency not read before its holder")
                out.append((d["step"]["on"], dep, "step"))
            else:
                each = d["each"]
                for m in self.members(l, each["key"], int(each["members"])):
                    out.append((each["on"], m, "member"))
        return out

    def conditional(self, l, memo):
        """Decision 6, amended: it awaits sent or settled a step that ended
        unknown or skipped, or awaits a conditional one; or it is an
        observation whose cause did not end expected."""
        if l in memo:
            return memo[l]
        why = None
        for on, dep, _ in self.awaited(l):
            if on in ("sent", "settled") and end_kind(self.end(dep)) in ("unknown", "skipped"):
                why = why or "doubtful"
            if self.conditional(dep, memo):
                why = why or "transitive"
        r = self.read(l)
        if r["action"] in ("observe", "poll"):
            cause = u(r["cause"])
            if cause is None or not (2 <= cause <= l):
                raise Incoherent("an observation's cause not read before it")
            if cause != l and end_kind(self.end(cause)) != "expected":
                why = why or "amended"
        memo[l] = why
        return why

    def resend_late(self, l):
        """Decision 16: a resend its last answer licensed, due more than J
        before the deadline, the step ended without it — a call's, not a
        poll's; the machine schedules it only then, and on time it leaves."""
        r = self.read(l)
        if r["action"] == "poll":
            return False
        e, ei = self.end(l), self.ends[l][0]
        dispatches = [i for i, k, _ in self.events[l] if k == "dispatch"]
        after_last = dispatches[-1] if dispatches else -1
        taken = [v for i, k, v in self.events[l]
                 if k == "answer" and v["ignored"] is None and after_last < i < ei]
        if not taken:
            return False
        v = taken[-1]
        licensed = (v["judged"] == "not_taken" and r["safety"] != "never") or (
            v["judged"] == "unknown" and r["safety"] == "idempotent")
        rel = self.released.get(l)
        if rel is None:
            return False
        due = u(v["at"]) + max(u(v["after"]) or 0, self.floor)
        return licensed and due + self.j < u(rel["deadline"])

    def undecided(self, l):
        """An unknown whose deadline the harness ate past J; decision 16: an
        end a late wake made, unknown or a refusal."""
        e = self.end(l)
        if end_kind(e) == "unexpected":
            return "resend" if self.resend_late(l) else None
        if end_kind(e) != "unknown":
            return None
        if e["fault"]:
            return "backstop"
        r = self.read(l)
        j = self.j
        if r["action"] != "observe" and e["attempts"] >= 1:
            last = e["attempts"]
            _, ei = None, self.ends[l][0]
            dispatched = [u(v["at"]) for i, k, v in self.events[l] if k == "dispatch" and v["attempt"] == last]
            sends = [u(v["at"]) for i, k, v in self.events[l] if k == "sent" and v["attempt"] == last and i < ei]
            if not sends or sends[0] - dispatched[0] > j:
                return "call"
        rel = self.released.get(l)
        if rel:
            i, rr, d = u(rel["intended"]), u(rel["released"]), u(rel["deadline"])
            if rr >= d and i < d and rr - i > j:
                return "read"
            if e["attempts"] >= 1 and rr < d and u(e["ended"]) > d:
                return "ended_late"
        if r["action"] in ("observe", "poll"):
            c = self.end(u(r["cause"]))
            if c["sent"] is None or u(c["sent"]) - u(c["intended"]) > j:
                return "cause"
        if self.resend_late(l):
            return "resend"
        return None

    def producer(self, l):
        return any(skip_of(self.end(m)) == ("missing_capture", l) for m in self.order)

    def classes(self):
        memo = {}
        out = []
        for l in self.order:
            e = self.end(l)
            kind = end_kind(e)
            for on, dep, how in self.awaited(l):
                if on == "expected" and end_kind(self.end(dep)) != "expected" and kind != "skipped":
                    raise Incoherent("awaited as expected a failed step, and not skipped")
            cond = self.conditional(l, memo)
            und = self.undecided(l)
            if und:
                REACHED[f"fired_{und}"] += 1
            if kind == "skipped":
                c = "consequent"
            elif cond:
                c = "inconclusive"
                REACHED[f"conditional_{cond}"] += 1
                if kind == "expected":
                    REACHED["conditional_and_expected"] += 1
                if any(self.ends[d][0] > self.ends[l][0] for _, d, _ in self.awaited(l)):
                    REACHED["conditional_ended_before_its_dependency"] += 1
            elif und:
                c = "undecided"
                REACHED[f"undecided_{und}"] += 1
            elif kind != "expected" or self.producer(l):
                c = "root"
                if kind == "expected":
                    REACHED["producer_root"] += 1
            else:
                c = "passed"
            REACHED[c] += 1
            out.append(c)
        return out


def verdict(lines, log):
    try:
        run = Run(lines, log)
        classes = run.classes()
    except AnotherLog:
        return "void:another_log", []
    except Incoherent:
        return "void:incoherent", []
    if run.faults:
        v = "void:fault"
    elif "root" in classes:
        v = "fail"
    elif "undecided" in classes:
        v = "void:undecided"
    elif all(c == "passed" for c in classes):
        v = "pass"
    else:
        v = "void:incoherent"
    REACHED[v] += 1
    return v, classes


def main():
    runs = 0
    for raw in sys.stdin:
        case = json.loads(raw)
        v, classes = verdict(case["journal"], case["log"])
        got = (case["verdict"], case["classes"])
        if (v, classes) != got:
            print(f"{case['name']}: by definition {v} {classes}, the fold {got[0]} {got[1]}")
            sys.exit(1)
        runs += 1
    print(f"{runs} verdicts agree; reached {dict(REACHED)}")


if __name__ == "__main__":
    main()
