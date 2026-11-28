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
import pathlib
import sys

REACHED = collections.Counter()

# Version 10's part table (K2b-7e-2b-3d-3b-3): the data file the fold's own
# table is held equal to.
PARTS_10 = json.loads(
    (pathlib.Path(__file__).resolve().parent.parent / "schema" / "parts-10.json").read_text()
)


class Incoherent(Exception):
    pass


class AnotherLog(Exception):
    pass


def u(v):
    return None if v is None else int(v)


def refuse(why):
    REACHED[why] += 1
    raise Incoherent(why)


def admission_10(catch_up, a):
    """Version 10's recheck of admission's evidence (ADR-0047 decision 22,
    K2b-7e-2b-3d-3b-3), each rule from the decision's sentence, in its order:
    what the journal alone makes a theorem of admission's own output."""
    table = {r["part"]: r for r in PARTS_10}
    m = a["memory"]
    kappa = catch_up
    if kappa >= 1000:
        refuse("admission_kappa")
    terms = m["terms"]
    n = sum(1 for t in terms if t["part"] == "runtime")
    # Each run's part once, each source's part once a source of the runtimes'.
    seen = set()
    for t in terms:
        r = table.get(t["part"])
        s = t["source"]
        placed = r is not None and (
            (r["scope"] == "global" and s is None)
            or (r["scope"] == "source" and s is not None and s < n)
        )
        if not placed or (t["part"], s) in seen:
            refuse("admission_term_set")
        seen.add((t["part"], s))
    runs = sum(1 for r in PARTS_10 if r["scope"] == "global")
    if len(seen) != runs + (len(PARTS_10) - runs) * n:
        refuse("admission_term_set")
    for t in terms:
        r = table[t["part"]]
        held = "gate" if isinstance(t["held"], dict) else t["held"]
        stated = r["stated"] in ("either", t["stated"])
        if t["phase"] != r["phase"] or t["basis"] != r["basis"] or held != r["held"] or not stated:
            refuse("admission_table")
    if any(isinstance(t["held"], dict) and t["held"]["gate"]["built"] for t in terms):
        refuse("admission_built")
    if any(int(t["count"]) * int(t["cost"]) + int(t["rest"]) != int(t["bytes"]) for t in terms):
        refuse("admission_bytes")

    def run(p):
        return next(t for t in terms if t["part"] == p)

    def at(p, s):
        return next(t for t in terms if t["part"] == p and t["source"] == s)

    model = m["model"]
    ended, live = int(model["ended_cell"]), int(model["live_cell"])
    if live < ended or model["event"] is None:
        refuse("admission_model")
    bound = [
        ("cells", ended),
        ("pending", live - ended),
        ("barriers", int(model["barrier"])),
        ("records", int(model["record"])),
        ("shell_attempts", int(model["attempt"])),
        ("decoder", int(model["decoder"])),
    ]
    runtimes = [t for t in terms if t["part"] == "runtime"]
    if any(int(run(p)["cost"]) != c for p, c in bound) or any(
        int(t["cost"]) != int(model["event"]) or int(t["rest"]) != int(model["runtime"])
        for t in runtimes
    ):
        refuse("admission_model")
    if int(run("decoder")["count"]) != (1 if int(run("reader")["count"]) > 0 else 0):
        refuse("admission_decoder")
    for s in range(n):
        c, io = at("connections", s), at("io", s)
        if (
            any(int(at(p, s)["count"]) != int(c["count"]) for p in ("io", "answers", "probes", "probes_io"))
            or at("probes", s)["cost"] != c["cost"]
            or at("probes_io", s)["cost"] != io["cost"]
        ):
            refuse("admission_shared")
    late = run("records")["count"]
    if any(run(p)["count"] != late for p in ("shell_attempts", "carrier_attempts", "decorations")):
        refuse("admission_shared")
    rows = a["sources"]
    ids = [r["source"] for r in rows]
    if any(i >= n for i in ids) or len(set(ids)) != len(ids):
        refuse("admission_rows")
    for s in range(n):
        sockets = int(at("connections", s)["count"])
        row = next((r for r in rows if r["source"] == s), None)
        if row is None:
            fits = sockets == 0
        else:
            fits = sockets == int(row["cap"]) and (
                a["assumed"]["runtimes"] or int(at("runtime", s)["count"]) == int(row["events"])
            )
        if not fits:
            refuse("admission_rows")
    total = {ph: sum(int(t["bytes"]) for t in terms if t["phase"] == ph)
             for ph in ("fixed", "stages", "gates", "run", "pool")}
    # The pool at the catch-up's rate, rounded up.
    if -(-total["pool"] * 1000 // (1000 - kappa)) != int(m["pool"]):
        refuse("admission_pool")
    f = total["fixed"]
    need = max(f + total["stages"], f + total["gates"], f + total["run"] + int(m["pool"]))
    if need != int(m["need"]):
        refuse("admission_need")
    stated = int(m["stated"])
    if stated < need or stated - need != int(m["slack"]):
        refuse("admission_slack")
    if stated - (f + total["run"]) != int(m["budget"]):
        refuse("admission_budget")
    unheld = []
    for t in terms:
        if isinstance(t["held"], dict) and t["part"] not in unheld:
            unheld.append(t["part"])
    if unheld != m["unheld"]:
        refuse("admission_unheld")
    top = m["limit"]["max"]
    if unheld and (top is None or top["depth"] != 0):
        refuse("admission_scope")
    if any(b is not None and stated > int(b["bytes"]) for b in (top, m["limit"]["high"])):
        refuse("admission_limit")


def end_kind(e):
    end = e["end"]
    return "skipped" if isinstance(end, dict) else end


def skip_of(e):
    end = e["end"]
    if not isinstance(end, dict):
        return None
    (why, line), = end["skipped"].items()
    return why, int(line)


def fault_class(v):
    """A fault line's class and its fields (version 6)."""
    c = v["class"]
    if isinstance(c, dict):
        (name, fields), = c.items()
        return name, fields if isinstance(fields, dict) else {"kind": fields}
    return c, {}


# The journal version this definition reads (K2b-7e-2b-3d-3b): another's
# rules are another's, so a journal of it is no case of this one.
VERSION = 10


class Run:
    def __init__(self, lines, log):
        header = lines[0]["header"]
        if header["journal"] != VERSION:
            raise SystemExit(f"journal version {header['journal']}: the definition reads {VERSION}")
        # Admission's evidence, by its version's rules (ADR-0047 decision 22).
        if header["admission"] is not None:
            admission_10(header["catch_up"], header["admission"])
        if header["log"] != log["sha256"]:
            raise AnotherLog()
        self.j = int(header["jitter"])
        self.floor = int(header["resend"])
        # Decision 18: no T0 only for a run a gate refused before it began.
        unstarted = header["t0"] is None
        body = lines[1:-1]
        self.reads, self.ends, self.released = {}, {}, {}
        self.facts = {}
        self.events = collections.defaultdict(list)  # line -> [(index, kind, value)]
        self.faults = {}
        # Decision 18: a run's stop — any fault but the backstop's — is the
        # journal's last line; a gate's refusal its only one, at 0.
        self.stop = None
        latest = 0
        for i, l in enumerate(body):
            (kind, v), = l.items()
            if kind == "fault":
                name, fields = fault_class(v)
                if name == "backstop":
                    self.faults[int(fields["line"])] = i
                else:
                    if i != len(body) - 1:
                        raise Incoherent("a line after the run's stop")
                    self.stop_coherent(name, fields, i, u(v["at"]), latest, unstarted)
                    self.stop = name
                latest = max(latest, u(v["at"]))
                continue
            latest = max(latest, u(v["at"]))
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
        if unstarted and self.stop != "gate":
            raise Incoherent("no T0, and no gate's refusal")
        # Stopped, a step read need not have ended.
        if set(self.ends) - set(self.reads):
            raise Incoherent("an end without its read")
        if self.stop is None and set(self.ends) != set(self.reads):
            raise Incoherent("a read without its end")
        if set(self.faults) - set(self.reads):
            raise Incoherent("a fault of a line not read")
        self.order = order
        for l in order:
            self.coherent(l)
        # The journal sound, it must be all of its log's run — a stopped one,
        # at most all of it.
        if len(order) > log["records"] or (self.stop is None and len(order) != log["records"]):
            raise AnotherLog()

    def stop_coherent(self, name, fields, i, at, latest, unstarted):
        """Decision 18: a gate's refusal the journal's only line, at 0, with no
        T0; any other stop no earlier than every line before it, the step it
        names read, and its attempt dispatched while the step is not ended."""
        if name == "gate":
            if i != 0 or at != 0 or not unstarted:
                raise Incoherent("a gate's refusal not the journal's only line")
            return
        if at < latest:
            raise Incoherent("a stop before the line above it")
        if "line" in fields:
            line = int(fields["line"])
            if line not in self.reads:
                raise Incoherent("a stop naming a line not read")
            if "attempt" in fields and line not in self.ends:
                out = sum(1 for _, kind, _ in self.events[line] if kind == "dispatch")
                if not (1 <= fields["attempt"] <= out):
                    raise Incoherent("a stop naming an attempt not out")

    def read(self, l):
        return self.reads[l][1]

    def end(self, l):
        return self.ends[l][1]

    def coherent(self, l):
        """Every end agrees with its step's events (decision 14: evidence); a
        step a stop left unended, its events so far (decision 18)."""
        ri, r = self.reads[l]
        ei, e = self.ends.get(l, (float("inf"), None))
        if ei < ri:
            raise Incoherent("ended before read")
        attempts, first_dispatch, sent, taken = 0, None, None, None
        last_dispatch, last_sent = None, None
        answered = None  # the last taken answer: (attempt, at, after)
        rel = None
        # Decision 17: the harness's lateness over the attempts — each one's
        # machine (dispatch at its wake's due) and hand-off (its task's first
        # poll after its dispatch); the last attempt's deadline; whether the
        # taken answer was the frontier's censor.
        harness, harness_sent, deadline, censored = 0, None, None, False
        # Version 5: the late answers found for the censor, and their brackets.
        founds = []
        # Version 7: a never-sent censor's closing reports (decision 19).
        reports = []
        for i, kind, v in self.events[l]:
            if i < ri:
                raise Incoherent("an event before its read")
            if kind == "sent" and i > ei:
                continue
            if kind == "answer" and i > ei:
                if v["ignored"] is None:
                    raise Incoherent("an answer taken after its end")
                if v["unsent"] is not None:
                    reports.append(v)
                    continue
                if v["found"] is not None:
                    founds.append((v, None))
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
                due = u(v["due"])
                if due > u(v["at"]):
                    raise Incoherent("dispatched before its wake was due")
                if v["attempt"] == 1 and due != u(rel["released"]):
                    raise Incoherent("a first attempt's wake not its release")
                if v["attempt"] > 1:
                    _, then, after = answered
                    if r["action"] == "poll":
                        if due < then:
                            raise Incoherent("a poll's wake before its answer")
                    elif due != then + max(after or 0, self.floor):
                        raise Incoherent("a resend's wake not its answer's wait")
                harness += u(v["at"]) - due
                deadline = u(v["deadline"])
                censored = False
                attempts = v["attempt"]
                first_dispatch = u(v["at"]) if first_dispatch is None else first_dispatch
                last_dispatch, last_sent, taken, answered = u(v["at"]), None, None, None
            elif kind == "sent":
                if v["attempt"] > attempts or v["attempt"] == 0:
                    raise Incoherent("a send of an attempt not dispatched")
                if not (u(v["handed"]) <= u(v["connected"]) <= u(v["at"])):
                    raise Incoherent("handed, connected and sent out of order")
                # Decision 18: the source thread's lag lies inside its path,
                # and is the harness's.
                if u(v["lag"]) > u(v["at"]) - u(v["handed"]):
                    raise Incoherent("a send's lag past its path")
                if v["attempt"] == attempts and last_sent is None:
                    if u(v["handed"]) < last_dispatch:
                        raise Incoherent("handed before its dispatch")
                    harness += u(v["handed"]) - last_dispatch + u(v["lag"])
                    harness_sent = harness if harness_sent is None else harness_sent
                if v["attempt"] == attempts:
                    last_sent = u(v["at"]) if last_sent is None else last_sent
                    sent = u(v["at"]) if sent is None else sent
            elif kind == "answer":
                if v["unsent"] is not None:
                    raise Incoherent("a closing report before its step's end")
                if v["found"] is not None:
                    founds.append((v, (censored, attempts, deadline, last_dispatch)))
                    continue
                if v["censored"] and (v["judged"] != "unknown" or v["status"] is not None):
                    raise Incoherent("a censor that says more than unknown")
                if v["ignored"] is None:
                    if v["attempt"] != attempts or attempts == 0:
                        raise Incoherent("an answer taken from an attempt not out")
                    if v["censored"] and u(v["at"]) != deadline:
                        raise Incoherent("a censor not at its attempt's deadline")
                    taken = v["judged"]
                    answered = (v["attempt"], u(v["at"]), u(v["after"]))
                    censored = v["censored"]
                elif v["attempt"] > attempts:
                    raise Incoherent("an answer of an attempt not dispatched")
        if e is None:
            return
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
            if u(r["planned"]) > u(e["intended"]):
                raise Incoherent("intended before its plan")
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
        self.woke(l, r, rel, e, attempts, answered)
        found_late = self.founds(founds, e, censored, attempts, deadline, last_dispatch)
        reader = 0
        if r["action"] in ("observe", "poll") and rel is not None:
            reader = u(rel["released"]) - u(rel["intended"])
        unsent_undecided = self.closing(
            l, e, reports, founds, censored, attempts, deadline, last_dispatch, last_sent, harness,
            reader)
        if founds and unsent_undecided is not None:
            found_late = True
        self.taken_attempts = attempts
        self.facts[l] = {
            "harness": harness,
            "harness_sent": harness_sent,
            "censored": censored,
            "last_sent": last_sent,
            "found_late": found_late,
            "unsent_undecided": unsent_undecided,
        }

    def closing(self, l, e, reports, founds, censored, attempts, deadline, dispatched, last_sent,
                harness, reader):
        """Decision 19, version 7: a never-sent censor — the last attempt
        censored, no send — owes one closing report in a run that ended: its
        path, after the deadline; or an answer found late for it. A Root iff
        the attempt was ready, the peer refused it by its own act, and the
        harness's lateness over the attempts, the hand-off, the thread's lag
        and an observation's reader's is at most J. None for any other end."""
        never_sent = (end_kind(e) == "unknown" and attempts >= 1 and censored
                      and last_sent is None and not e["fault"])
        if not never_sent:
            if reports:
                raise Incoherent("a closing report without its never-sent censor")
            return None
        if len(reports) + len(founds) > 1:
            raise Incoherent("two closing reports for one censor")
        if not reports and not founds:
            if self.stop is None:
                raise Incoherent("a never-sent censor without its closing report")
            return True
        if founds:
            REACHED["unsent_found"] += 1
            return True
        v = reports[0]
        un = v["unsent"]
        if (v["judged"] != "unknown" or v["status"] is not None or v["censored"]
                or v["found"] is not None):
            raise Incoherent("a closing report with an answer's content")
        if v["attempt"] != attempts or u(v["at"]) <= deadline:
            raise Incoherent("a closing report against its censor")
        handed, connected = u(un["handed"]), u(un["connected"])
        ready = dispatched if handed is None else handed
        if (ready < dispatched
                or (connected is not None and (handed is None or connected < ready or connected > u(v["at"])))
                or u(un["lag"]) > u(v["at"]) - ready
                or (un["refused_by"] is not None) != (un["refused"] > 0)):
            raise Incoherent("a closing report against its path")
        late = harness + (ready - dispatched) + u(un["lag"]) + reader > self.j
        if handed is None:
            REACHED["unsent_unready"] += 1
            return True
        if un["refused"] == 0:
            REACHED["unsent_unrefused"] += 1
            return True
        if late:
            REACHED["unsent_late"] += 1
            return True
        REACHED["unsent_root"] += 1
        return False

    def founds(self, founds, e, censored, attempts, deadline, last_dispatch):
        """Decision 17, version 5: a late answer found for its attempt's censor —
        ignored, at most one a censor, absent after the attempt's dispatch and
        before the answer, the deadline the attempt's; found before the step's
        end only while its taken answer is that censor, after it only for an
        end the censor made. Late if absent before the deadline and found more
        than J after."""
        if len(founds) > 1:
            raise Incoherent("two late answers found for one censor")
        late = False
        for v, live in founds:
            f = v["found"]
            absent, fd, at = u(f["absent"]), u(f["deadline"]), u(v["at"])
            if v["ignored"] is None:
                raise Incoherent("a found answer taken")
            if not (absent <= at and fd < at):
                raise Incoherent("a found answer inside its own bracket")
            if live is not None:
                c, n, d, dispatched = live
            else:
                c, n, d, dispatched = censored, attempts, deadline, last_dispatch
                if end_kind(e) != "unknown":
                    raise Incoherent("a found answer of a step its censor did not end")
            if not c or v["attempt"] != n or fd != d or absent < dispatched:
                raise Incoherent("a found answer against its censor")
            late = absent < fd and at - absent > self.j
        return late

    def woke(self, l, r, rel, e, attempts, answered):
        """Decision 17: the wake that ended a step. None for an input's end or a
        skip; a backstop's exempt. With no attempt, its release, the end at or
        past its deadline. A poll's at most its deadline, at it only for an end
        there, before it only for an end at or past it. A call's fallback at its
        answer and its wait, more than J before its deadline. Never after the
        end."""
        w = u(e["wake"])
        if w is None or e["fault"]:
            return
        kind = end_kind(e)
        if kind in ("expected", "skipped"):
            raise Incoherent("an expected or skipped end by a wake")
        if rel is None:
            raise Incoherent("a wake on a step never released")
        d, ended = u(rel["deadline"]), u(e["ended"])
        if w > ended:
            raise Incoherent("a wake after the end it made")
        if attempts == 0:
            if w != u(rel["released"]) or ended < d:
                raise Incoherent("a first dispatch's wake not its release past the deadline")
        elif r["action"] == "poll":
            if w > d or (w == d and ended != d) or (w < d and ended < d):
                raise Incoherent("a poll's wake against its deadline")
        else:
            _, then, after = answered
            if w != then + max(after or 0, self.floor) or w + self.j >= d:
                raise Incoherent("a fallback's wake against its licence")

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

    def harness_late(self, l):
        """Decision 17: the harness's lateness on a step — its attempts'
        machine and hand-off, and for an observation its reader's — past J."""
        r, f = self.read(l), self.facts[l]
        rel = self.released.get(l)
        reader = 0
        if r["action"] in ("observe", "poll") and rel is not None:
            reader = u(rel["released"]) - u(rel["intended"])
        return f["harness"] + reader > self.j

    def cause_late(self, c):
        """An observation's cause sent late by the harness, or never: its
        reader's lateness and its first send's harness lateness, past J."""
        e = self.end(c)
        if e["sent"] is None:
            return True
        rel = self.released.get(c)
        reader = u(rel["released"]) - u(rel["intended"]) if rel else 0
        return reader + (self.facts[c]["harness_sent"] or 0) > self.j

    def undecided(self, l):
        """Decision 17: undecided only what the harness could have caused —
        an end by a wake it took past the wake's deadline; an unknown censored
        at its deadline, or never sent, its harness late; an unknown read past
        its deadline; an observation whose cause the harness sent late; a
        backstop's end."""
        e, r, f = self.end(l), self.read(l), self.facts[l]
        kind = end_kind(e)
        if kind not in ("unexpected", "unknown"):
            return None
        rel = self.released.get(l)
        if not e["fault"] and e["wake"] is not None and rel is not None and u(e["wake"]) < u(rel["deadline"]):
            return "wake"
        if kind == "unexpected":
            return None
        if e["fault"]:
            return "backstop"
        if f["found_late"]:
            return "found"
        if e["attempts"] >= 1 and f["censored"]:
            if f["last_sent"] is None:
                if f["unsent_undecided"]:
                    return "censor"
            elif self.harness_late(l):
                return "censor"
        if rel:
            i, rr, d = u(rel["intended"]), u(rel["released"]), u(rel["deadline"])
            if rr >= d and i < d and rr - i > self.j:
                return "read"
        cause = u(r["cause"]) if r["action"] in ("observe", "poll") else None
        if cause is not None and cause != l and self.cause_late(cause):
            return "cause"
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
        # Decision 18: a stopped run is classed not at all.
        classes = [] if run.stop else run.classes()
    except AnotherLog:
        return "void:another_log", [], None, None
    except Incoherent as e:
        why = e.args[0] if e.args else None
        return "void:incoherent", [], None, why if str(why).startswith("admission_") else None
    if run.stop:
        REACHED[f"stop_{run.stop}"] += 1
        v = "void:fault"
    elif run.faults:
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
    return v, classes, run.stop, None


def main():
    runs = 0
    for raw in sys.stdin:
        case = json.loads(raw)
        v, classes, stop, why = verdict(case["journal"], case["log"])
        got = (case["verdict"], case["classes"], case.get("stop"), case.get("why"))
        if (v, classes, stop, why) != got:
            print(f"{case['name']}: by definition {v} {classes} {stop} {why}, the fold {got[0]} {got[1]} {got[2]} {got[3]}")
            sys.exit(1)
        runs += 1
    print(f"{runs} verdicts agree; reached {dict(REACHED)}")


if __name__ == "__main__":
    main()
