//! The REST carrier (ADR-0047 decision 16): an attempt over hyper's own
//! HTTP/1.1 connection, no client or pool of anyone else's.
//!
//! An attempt: its frame (a function of its fills, the first dispatch's
//! instant among them, so a resend's is the same bytes), the stand's route,
//! the correlation id; a connection from its source's pool, or a new one; the
//! request handed to it. Until a byte of it leaves, a request handed back —
//! or a connection closed under it — is no end: the next connection takes it.
//! Its last byte leaving is `Sent` (`io`); its answer arrives at its last body
//! frame — or once the cap is passed, its body unread and its connection
//! closed — and is judged under the frontier's floor. An error once a byte
//! left is the system's Unknown, `Broken`, at the error.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::{Future, poll_fn};

use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use ascetic_ddd_command_log::answer::{self, Reply};
use ascetic_ddd_command_log::fills::Fills;
use ascetic_ddd_command_log::record::{Action, Header, Operation, Record};
use ascetic_ddd_command_log::render::{self, RunSecret};
use ascetic_ddd_command_log::request::{self, Frame};
use ascetic_ddd_scheduler::judge::{Contract, Transport, judge};
use ascetic_ddd_scheduler::machine::Refusal;
use bytes::Bytes;
use http_body::{Body, Frame as BodyFrame, SizeHint};
use hyper::client::conn::http1::{self, SendRequest};
use tokio::task::AbortHandle;

use crate::carrier::{Carrier, Needs, Unprobed};
use crate::clock::Clock;
use crate::decorate::{Decoration, Decorator, Undecorated};
use crate::io::{Held, Io, Probe, Slot, Sockets};
use crate::stamps::Attempt;
use crate::stand::{Connect, Egress, Opening, Refused, Route, Stand, StandConnect, Unfit};
use crate::turn;

/// An attempt's looks around its deadline (decision 17): one at `D − J/2`,
/// the last at `D + 1` — each a verified turn of its runtime's driver, so an
/// answer already in the host is read, and the instant before a look that
/// finds it incomplete is where it is known absent.
struct Looks {
    pre: Option<u64>,
    past: u64,
}

impl Looks {
    /// The next look's instant.
    fn next(&self) -> u64 {
        self.pre.unwrap_or(self.past)
    }

    /// The look done: whether it was the last.
    fn done(&mut self) -> bool {
        self.pre.take().is_none()
    }
}

/// What every frame of a run is filled from beside its step and the run's
/// namespace (its attempt's: decision 20).
pub struct Config {
    /// The run's secret.
    pub secret: RunSecret,
    /// The least microseconds between an attempt's consecutive connects,
    /// its first at once (decision 16): a refused stand is not hammered.
    pub connect_floor: u64,
    /// How long before the profile's idle a connection is retired, unused:
    /// the gateway closes it at its idle, perhaps as it is reused.
    pub idle_margin: u64,
}

/// The least microseconds between an attempt's connects, by default.
pub const CONNECT_FLOOR: u64 = 10_000;

/// How long before the profile's idle a connection is retired, by default.
pub const IDLE_MARGIN: u64 = 1_000_000;

impl Config {
    /// The run's secret; the connect floor and the idle margin by default.
    /// T0's wall instant is its clock's, read at each attempt: a carrier
    /// built before T0 stamps no frame by an earlier one (decision 18); the
    /// namespace its attempt's.
    pub fn new(secret: RunSecret) -> Config {
        Config {
            secret,
            connect_floor: CONNECT_FLOOR,
            idle_margin: IDLE_MARGIN,
        }
    }
}

/// A request's body: its bytes in one frame, ready at once (`io`).
struct Outgoing {
    data: Option<Bytes>,
}

impl Body for Outgoing {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<BodyFrame<Bytes>, Infallible>>> {
        Poll::Ready(self.data.take().map(|d| Ok(BodyFrame::data(d))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.as_ref().map_or(0, |d| d.len() as u64))
    }
}

/// A connection: hyper's half to send on, and the probe of its bytes.
struct Conn {
    sender: SendRequest<Outgoing>,
    probe: Arc<Probe>,
    /// Since when it waits in its pool.
    idle_since: u64,
    /// Its task: aborted, its socket dropped — reset, at zero linger
    /// (decision 19).
    task: AbortHandle,
    /// Its hold on its socket's place: dropped unpooled, the socket dying.
    _held: Held,
}

impl Conn {
    /// Reset, as the emulated client's at its timeout (decision 19): the
    /// gateway cancels upstream.
    fn reset(self) {
        self.task.abort();
    }
}

/// A source's idle connections by endpoint, the latest used first.
#[derive(Default)]
struct Pool {
    idle: Mutex<BTreeMap<String, Vec<Conn>>>,
}

impl Pool {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Vec<Conn>>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The latest idle connection to `endpoint` its peer has not closed and
    /// idle less than `retire` µs at `now`; the others retired.
    fn checkout(&self, endpoint: &str, now: u64, retire: u64) -> Option<Conn> {
        let mut idle = self.lock();
        let conns = idle.get_mut(endpoint)?;
        while let Some(c) = conns.pop() {
            if !c.sender.is_closed() && now.saturating_sub(c.idle_since) < retire {
                return Some(c);
            }
        }
        None
    }

    fn checkin(&self, endpoint: &str, mut conn: Conn, now: u64) {
        conn.idle_since = now;
        self.put(endpoint, conn);
    }

    /// The connection idle longest, to any endpoint: evicted at the cap
    /// (decision 19).
    fn evict(&self) -> Option<Conn> {
        let mut idle = self.lock();
        let endpoint = idle
            .iter()
            .filter_map(|(e, conns)| conns.first().map(|c| (c.idle_since, e.clone())))
            .min()?
            .1;
        let conns = idle.get_mut(&endpoint)?;
        (!conns.is_empty()).then(|| conns.remove(0))
    }

    /// Back unused, idle since when it was (decision 19).
    fn put(&self, endpoint: &str, conn: Conn) {
        self.lock()
            .entry(endpoint.to_owned())
            .or_default()
            .push(conn);
    }
}

struct Inner<S, E, D> {
    header: Arc<Header>,
    stand: S,
    egress: E,
    decorator: D,
    config: Config,
    /// By source.
    pools: Vec<Pool>,
    /// By source: its sockets under its cap (decision 19).
    sockets: Vec<Arc<Sockets>>,
    /// By source: the endpoints its stand declares (decision 20).
    endpoints: Vec<std::collections::BTreeSet<String>>,
}

/// The REST carrier: see the [module documentation](self).
pub struct RestCarrier<S, E, D = Undecorated> {
    inner: Arc<Inner<S, E, D>>,
}

impl<S: Stand, E: Egress> RestCarrier<S, E, Undecorated> {
    /// Carries the calls of the log `header` heads to `stand`'s routes,
    /// connecting by `egress`, decorating none.
    pub fn new(header: Arc<Header>, stand: S, egress: E, config: Config) -> RestCarrier<S, E> {
        RestCarrier::decorated(header, stand, egress, Undecorated, config)
    }
}

impl<S: Stand, E: Egress, D: Decorator> RestCarrier<S, E, D> {
    /// [`RestCarrier::new`], each attempt decorated by `decorator`.
    pub fn decorated(
        header: Arc<Header>,
        stand: S,
        egress: E,
        decorator: D,
        config: Config,
    ) -> RestCarrier<S, E, D> {
        let sources = usize::try_from(header.profile.sources.max(1)).unwrap_or(1);
        let pools = (0..sources).map(|_| Pool::default()).collect();
        let sockets = (0..sources)
            .map(|s| Sockets::new(egress.ports(u32::try_from(s).unwrap_or(u32::MAX))))
            .collect();
        let endpoints = (0..sources)
            .map(|s| {
                stand
                    .endpoints(&header, u32::try_from(s).unwrap_or(u32::MAX))
                    .into_iter()
                    .collect()
            })
            .collect();
        RestCarrier {
            inner: Arc::new(Inner {
                header,
                stand,
                egress,
                decorator,
                config,
                pools,
                sockets,
                endpoints,
            }),
        }
    }
}

impl<C: Clock, S: Stand, E: Egress, D: Decorator> Carrier<C> for RestCarrier<S, E, D> {
    fn carries(&self, header: &Header) -> Result<(), String> {
        // The plan's header, not one of its own (decision 20).
        if !std::ptr::eq(header, &*self.inner.header) {
            return Err("the carrier's header is not the plan's".to_owned());
        }
        if !header.channels.is_empty() {
            return Err("a log with channels: the bus is K3's".to_owned());
        }
        if !header.calls.is_empty() {
            return Err("a log with host calls: they are K3's".to_owned());
        }
        if header.profile.idle <= self.inner.config.idle_margin {
            return Err(format!(
                "the profile's idle {} µs within the idle margin {} µs",
                header.profile.idle, self.inner.config.idle_margin
            ));
        }
        if !self.inner.decorator.decorates()
            && let Some(op) = header.operations.iter().find(|o| !o.decorated.is_empty())
        {
            return Err(format!("operation {} decorates, and no decorator", op.name));
        }
        // The egress carries every source, and each reaches every endpoint
        // its stand declares for it — no packet sent (decision 20).
        let told = |u: Unfit| format!("{}: {}", u.cannot, u.detail);
        let sources = u32::try_from(self.inner.endpoints.len()).unwrap_or(u32::MAX);
        self.inner.egress.admits(sources).map_err(told)?;
        for (source, endpoints) in (0..sources).zip(&self.inner.endpoints) {
            for endpoint in endpoints {
                self.inner.egress.reaches(source, endpoint).map_err(told)?;
            }
        }
        Ok(())
    }

    /// A socket an attempt not ended, under the plan's bound its pending
    /// steps (one HTTP/1.1 request a connection); its cap the egress's.
    fn needs(
        &self,
        peaks: &ascetic_ddd_command_log::validate::Peaks,
        source: u32,
    ) -> Result<Needs, String> {
        let sockets = usize::try_from(source)
            .ok()
            .and_then(|s| self.inner.sockets.get(s))
            .ok_or_else(|| format!("no source {source} in the carrier's own header"))?;
        Ok(Needs::Sockets {
            needed: peaks.pending,
            cap: sockets.cap(),
        })
    }

    /// One connect to each endpoint `source` declares, all at once, each
    /// raced against the bound; a stream got, dropped — reset, no byte.
    fn probe(
        &self,
        source: u32,
        clock: Arc<C>,
        bound: u64,
    ) -> impl Future<Output = Vec<Unprobed>> + Send + 'static {
        let inner = self.inner.clone();
        async move {
            let declared: Vec<String> = usize::try_from(source)
                .ok()
                .and_then(|s| inner.endpoints.get(s))
                .map(|e| e.iter().cloned().collect())
                .unwrap_or_default();
            let until = clock.now().saturating_add(bound);
            let probes = declared
                .into_iter()
                .enumerate()
                .map(|(endpoint, name)| {
                    let (inner, clock) = (inner.clone(), clock.clone());
                    let probe: Pin<Box<dyn Future<Output = Option<Unprobed>> + Send>> =
                        Box::pin(async move {
                            let route = Route {
                                authority: String::new(),
                                endpoint: name,
                            };
                            let kind = tokio::select! {
                                biased;
                                c = inner.egress.connect(source, &route) => match c {
                                    Ok(stream) => {
                                        drop(stream);
                                        return None;
                                    }
                                    Err(Connect::Stand(why)) => why.name().to_owned(),
                                    Err(Connect::Harness { cannot, .. }) => cannot.replace('.', "_"),
                                },
                                () = clock.sleep_until(until) => "unanswered".to_owned(),
                            };
                            Some(Unprobed { endpoint, kind })
                        });
                    probe
                })
                .collect();
            all(probes).await.into_iter().flatten().collect()
        }
    }

    fn carry(&self, attempt: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.clone();
        async move { inner.attempt(attempt).await }
    }

    fn gate(
        &self,
        i: usize,
        opening: Opening<'_>,
    ) -> impl Future<Output = Option<Result<String, Refused>>> + Send {
        self.inner.stand.gate(i, opening)
    }
}

/// Every future of `futures` to its end, polled together: their outputs in
/// their order.
async fn all<T>(futures: Vec<Pin<Box<dyn Future<Output = T> + Send>>>) -> Vec<T> {
    let mut pending: Vec<Option<Pin<Box<dyn Future<Output = T> + Send>>>> =
        futures.into_iter().map(Some).collect();
    let mut done: Vec<Option<T>> = pending.iter().map(|_| None).collect();
    poll_fn(|cx| {
        let mut all_done = true;
        for (slot, out) in pending.iter_mut().zip(done.iter_mut()) {
            if let Some(f) = slot {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => {
                        *out = Some(v);
                        *slot = None;
                    }
                    Poll::Pending => all_done = false,
                }
            }
        }
        if all_done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    done.into_iter().flatten().collect()
}

/// A socket's place at its source's cap (decision 19).
enum Place {
    /// Under the cap: taken.
    Taken(Slot),
    /// A dying socket's place, freed soon — or the idle connection evicted,
    /// once it drops.
    Dying,
    /// Every socket in flight: a place freed, or a connection idled, awaited.
    Full,
}

impl Place {
    /// An attempt that left waiting for it: where it stood.
    fn code(&self) -> &'static str {
        match self {
            Place::Taken(_) | Place::Dying => "rest.cap.dying",
            Place::Full => "rest.cap.full",
        }
    }
}

/// A place for a new socket of `sockets`: under the cap, taken; at it, a
/// dying socket awaited, else the connection idle longest evicted — reset,
/// its place freed once it drops; none idle, full.
fn place(sockets: &Arc<Sockets>, pool: &Pool) -> Place {
    if let Some(slot) = sockets.take() {
        return Place::Taken(slot);
    }
    if sockets.dying() > 0 {
        return Place::Dying;
    }
    match pool.evict() {
        Some(idle) => {
            idle.reset();
            Place::Dying
        }
        None => Place::Full,
    }
}

/// The peer's own refusals of an attempt's connects (decision 19): refused,
/// reset, aborted, or a fresh connection closed before a byte — a connect
/// timed out or unreachable is no act of its, perhaps the harness's own
/// network.
#[derive(Clone, Copy, Debug, Default)]
struct Refusals {
    count: u32,
    last: Option<Refusal>,
}

impl Refusals {
    fn note(&mut self, why: StandConnect) {
        let r = match why {
            StandConnect::Refused => Refusal::Refused,
            StandConnect::Reset => Refusal::Reset,
            StandConnect::Aborted => Refusal::Aborted,
            StandConnect::TimedOut | StandConnect::Unreachable => return,
        };
        self.count = self.count.saturating_add(1);
        self.last = Some(r);
    }

    fn closed(&mut self) {
        self.count = self.count.saturating_add(1);
        self.last = Some(Refusal::Closed);
    }
}

/// Its last look passed with no answer (decision 19): sent, censored, nothing
/// more; never sent, its closing report, pushed no earlier than a microsecond
/// past its deadline — where it stood, `code`.
async fn leave<C: Clock>(a: Attempt<C>, refusals: Refusals, code: &'static str) {
    if a.was_sent() {
        return a.unanswered();
    }
    let past = a.deadline().saturating_add(1);
    if a.clock().now() < past {
        a.sleep_until(past).await;
    }
    let report = a.report(code, refusals.count, refusals.last);
    a.left(report);
}

/// The request of `frame` to `route`, its correlation header and its
/// decorated ones set.
fn build(
    frame: &Frame,
    route: &Route,
    correlation: (&str, &str),
    decorated: &[(String, String)],
) -> Result<http::Request<Outgoing>, http::Error> {
    let mut b = http::Request::builder()
        .method(frame.method.as_str())
        .uri(frame.target.as_str())
        .header(http::header::HOST, route.authority.as_str());
    for (name, value) in &frame.headers {
        b = b.header(name.as_str(), value.as_str());
    }
    b = b.header(correlation.0, correlation.1);
    for (name, value) in decorated {
        b = b.header(name.as_str(), value.as_str());
    }
    b.body(Outgoing {
        data: frame.body.clone().map(Bytes::from),
    })
}

impl<S: Stand, E: Egress, D: Decorator> Inner<S, E, D> {
    async fn connect<C: Clock>(
        &self,
        a: &Attempt<C>,
        source: u32,
        route: &Route,
        slot: Slot,
    ) -> Result<Conn, Connect> {
        let stream = self.egress.connect(source, route).await?;
        let held = slot.held();
        let probe = Arc::new(Probe::default());
        // hyper's HTTP/1.1 handshake does no I/O: its failure is the
        // harness's.
        let (sender, connection) = http1::handshake(Io::new(stream, probe.clone(), slot))
            .await
            .map_err(|e| Connect::Harness {
                cannot: "rest.handshake",
                detail: e.to_string(),
            })?;
        // The connection's own task, on the source's runtime: the attempt's,
        // kept with the run's for a stop to abort (decision 18).
        let task = a.spawn_connection(probe.clone(), async move {
            let _ = connection.await;
        });
        Ok(Conn {
            sender,
            probe,
            idle_since: a.clock().now(),
            task,
            _held: held,
        })
    }

    async fn attempt<C: Clock>(&self, mut a: Attempt<C>) {
        let record = a.shared_record();
        let Some(rest) = request::rest_of(&record.action) else {
            return a.fault("rest.action", "not a REST call".to_owned());
        };
        let header = &self.header;
        let Some(op) = header.operations.iter().find(|o| o.name == rest.op) else {
            return a.fault("rest.operation", format!("no operation {}", rest.op));
        };
        let Some((pool, sockets)) = usize::try_from(rest.source)
            .ok()
            .and_then(|s| self.pools.get(s).zip(self.sockets.get(s)))
        else {
            return a.fault("rest.source", format!("no source {}", rest.source));
        };
        let captured = a.captured().to_vec();
        let run_start = a.clock().wall();
        let wall = |at: u64| run_start.saturating_add(i64::try_from(at).unwrap_or(i64::MAX));
        let fills = Fills {
            namespace: a.namespace(),
            secret: &self.config.secret,
            run_start,
            // A call's frame is its first attempt's, resent byte for byte; a
            // poll's is its own (decision 16).
            sent: wall(if matches!(record.action, Action::Observe(_)) {
                a.dispatched()
            } else {
                a.first_dispatched()
            }),
            captured: &captured,
        };
        let frame = match request::frame(op, rest, &fills, &header.profile.correlation) {
            Ok(f) => f,
            Err(e) => return a.fault("rest.frame", e.to_string()),
        };
        let route = match self.stand.route(&record, rest) {
            Ok(r) => r,
            Err(why) => return a.fault("rest.route", why),
        };
        // Only an endpoint admitted and probed for its source (decision 20).
        if !self
            .endpoints
            .get(usize::try_from(rest.source).unwrap_or(usize::MAX))
            .is_some_and(|e| e.contains(&route.endpoint))
        {
            return a.fault(
                "rest.endpoint",
                format!(
                    "endpoint {} not among source {}'s declared",
                    route.endpoint, rest.source
                ),
            );
        }
        let id = render::correlation(a.namespace(), &record.step, a.attempt());
        // Decorated once an attempt (decision 16): only the operation's
        // declared names, each a header's value.
        let view = Decoration {
            record: &record,
            attempt: a.attempt(),
            caller: rest.caller.as_ref(),
            operation: op,
            route: &route,
            frame: &frame,
            correlation: &id,
            deadline: a.deadline(),
            wall: wall(a.clock().now()),
        };
        // Every await of an attempt races its deadline: a decorator that
        // never answers ends it unsent, censored, never past it (decision 18).
        let past = a.deadline().saturating_add(1);
        let decorated = tokio::select! {
            biased;
            d = self.decorator.decorate(view) => match d {
                Ok(d) => d,
                Err(why) => return a.fault("rest.decorator", why),
            },
            () = a.sleep_until(past) => {
                return leave(a, Refusals::default(), "rest.decorating").await;
            }
        };
        for (name, value) in &decorated {
            if !op.decorated.iter().any(|d| d.eq_ignore_ascii_case(name)) {
                return a.fault(
                    "rest.undeclared_header",
                    format!("a decorated header {name} its operation does not declare"),
                );
            }
            if http::HeaderName::from_bytes(name.as_bytes()).is_err()
                || http::HeaderValue::from_str(value).is_err()
            {
                return a.fault(
                    "rest.bad_header",
                    format!("a decorated header {name} not a header"),
                );
            }
        }
        // Ready to connect: its frame built and decorated — the harness's
        // from the dispatch (decision 17).
        a.decorated();
        let deadline = a.deadline();
        // An answer stamped at the deadline is taken: the attempt leaves only
        // past it, and the frontier censors at it what had none.
        let mut looks = Looks {
            pre: Some(deadline.saturating_sub(header.profile.jitter / 2)),
            past,
        };
        let retire = header.profile.idle.saturating_sub(self.config.idle_margin);
        let mut connected_last: Option<u64> = None;
        // The peer's own refusals, for a never-sent attempt's report.
        let mut refusals = Refusals::default();
        // Until a byte of it leaves: a connection, the request handed to it.
        let (response, conn) = loop {
            if a.clock().now() >= deadline {
                return leave(a, refusals, "rest.connecting").await;
            }
            let mut fresh = false;
            let mut conn = match pool.checkout(&route.endpoint, a.clock().now(), retire) {
                Some(c) => {
                    sockets.served();
                    c
                }
                None => {
                    fresh = true;
                    // Connects a floor apart, the first at once: a stand
                    // refusing them is not hammered, and the frontier censors
                    // an attempt never connected (decision 16).
                    if let Some(last) = connected_last {
                        let next = last.saturating_add(self.config.connect_floor);
                        if next >= deadline {
                            return leave(a, refusals, "rest.floor").await;
                        }
                        a.sleep_until(next).await;
                    }
                    // Its socket's place under the source's cap (decision
                    // 19): a dying one awaited, else the longest idle
                    // evicted; every socket in flight, a place freed or a
                    // connection idled awaited — then its endpoint's pool
                    // again. The wait is the harness's, on the path's lag;
                    // a source whose waiters got nothing for the route
                    // timeout, a stuck cap, stops the run.
                    let mut freed = pin!(sockets.freed());
                    freed.as_mut().enable();
                    let slot = match place(sockets, pool) {
                        Place::Taken(slot) => slot,
                        waiting => {
                            let from = a.clock().now();
                            let since = sockets.starving(from);
                            if from.saturating_sub(since) >= header.profile.route_timeout {
                                return a.fault(
                                    "egress.cap",
                                    format!(
                                        "no connection for {} µs, {} sockets open, the cap {}",
                                        from.saturating_sub(since),
                                        sockets.open(),
                                        sockets.cap()
                                    ),
                                );
                            }
                            let before = a.lag();
                            tokio::select! {
                                biased;
                                () = freed => {}
                                () = a.sleep_until(past) => {
                                    a.waited(from, before);
                                    return leave(a, refusals, waiting.code()).await;
                                }
                            }
                            a.waited(from, before);
                            continue;
                        }
                    };
                    sockets.served();
                    connected_last = Some(a.clock().now());
                    let connecting = self.connect(&a, rest.source, &route, slot);
                    match tokio::select! {
                        biased;
                        c = connecting => c,
                        () = a.sleep_until(past) => {
                            return leave(a, refusals, "rest.connecting").await;
                        }
                    } {
                        Ok(c) => c,
                        // The stand's: retried a floor apart, the peer's own
                        // refusals counted (decision 19).
                        Err(Connect::Stand(why)) => {
                            refusals.note(why);
                            continue;
                        }
                        // The harness's: the run stopped.
                        Err(Connect::Harness { cannot, detail }) => {
                            return a.fault(cannot, detail);
                        }
                    }
                }
            };
            let ready = tokio::select! {
                biased;
                r = poll_fn(|cx| conn.sender.poll_ready(cx)) => r.is_ok(),
                () = a.sleep_until(past) => {
                    return leave(a, refusals, "rest.pooling").await;
                }
            };
            if !ready {
                continue;
            }
            // None handed at or past the deadline (decision 19): the
            // connection back unused.
            if a.clock().now() >= deadline {
                pool.put(&route.endpoint, conn);
                sockets.idled();
                return leave(a, refusals, "rest.pooling").await;
            }
            let req = match build(
                &frame,
                &route,
                (&header.profile.correlation, &id),
                &decorated,
            ) {
                Ok(r) => r,
                Err(e) => return a.fault("rest.request", e.to_string()),
            };
            // Handed to a ready connection: the connect's lateness, the
            // stand's, ends here (decision 17).
            a.connected();
            // Armed and handed over in this one poll: no flush between.
            conn.probe
                .arm(Box::new(a.sent_hook()), a.path(), a.sending());
            let mut sent = pin!(conn.sender.try_send_request(req));
            let outcome = loop {
                tokio::select! {
                    biased;
                    r = &mut sent => break r,
                    () = a.clock().sleep_until(looks.next()) => {
                        let b0 = a.clock().now();
                        if let Some(r) = turn::after_turn(&mut sent).await {
                            break r;
                        }
                        a.absent_before(b0);
                        // Censored by the frontier at the deadline; the
                        // connection reset, no byte of it out since.
                        if looks.done() {
                            conn.reset();
                            return leave(a, refusals, "rest.writing").await;
                        }
                    }
                }
            };
            match outcome {
                Ok(response) => break (response, conn),
                Err(mut e) => {
                    // Handed back, or no byte of it accepted: no end; its
                    // path's lag goes on on this task (decision 18).
                    if e.take_message().is_some() || conn.probe.written() == 0 {
                        // A fresh connection the peer closed before a byte:
                        // its refusal, by its own act (decision 19).
                        if fresh {
                            refusals.closed();
                        }
                        a.back();
                        continue;
                    }
                    return self.judged(a, &record, op, &fills, &Transport::Broken);
                }
            }
        };
        let (parts, mut incoming) = response.into_parts();
        let cap = usize::try_from(header.profile.answer_cap).unwrap_or(usize::MAX);
        let mut body = Vec::new();
        let mut whole = true;
        loop {
            let next = tokio::select! {
                biased;
                f = poll_fn(|cx| Pin::new(&mut incoming).poll_frame(cx)) => Some(f),
                () = a.clock().sleep_until(looks.next()) => None,
            };
            let next = match next {
                Some(f) => f,
                None => {
                    // A look: a verified turn, then the body as it stands; a
                    // frame it brings is read and the look stays due.
                    let b0 = a.clock().now();
                    let mut frame = poll_fn(|cx| Pin::new(&mut incoming).poll_frame(cx));
                    match turn::after_turn(&mut frame).await {
                        Some(f) => f,
                        None => {
                            a.absent_before(b0);
                            if looks.done() {
                                conn.reset();
                                return leave(a, refusals, "rest.reading").await;
                            }
                            continue;
                        }
                    }
                }
            };
            match next {
                None => break,
                Some(Ok(f)) => {
                    if let Ok(data) = f.into_data() {
                        if body.len().saturating_add(data.len()) > cap {
                            // Past the cap: unread, and the connection closed.
                            whole = false;
                            break;
                        }
                        body.extend_from_slice(&data);
                    }
                }
                Some(Err(_)) => {
                    return self.judged(a, &record, op, &fills, &Transport::Broken);
                }
            }
        }
        a.arrived();
        let headers: Vec<(String, Vec<u8>)> = parts
            .headers
            .iter()
            .map(|(n, v)| (n.as_str().to_owned(), v.as_bytes().to_vec()))
            .collect();
        let reply = Reply {
            status: parts.status.as_u16(),
            headers: &headers,
            body: whole.then_some(body.as_slice()),
        };
        let read = answer::read(&reply);
        let idle_since = a.clock().now();
        self.judged(a, &record, op, &fills, &Transport::Answered(read));
        if whole && !conn.sender.is_closed() {
            pool.checkin(&route.endpoint, conn, idle_since);
            sockets.idled();
        }
    }

    /// The attempt judged on what came: arrived now, if it had not.
    fn judged<C: Clock>(
        &self,
        mut a: Attempt<C>,
        record: &Record,
        op: &Operation,
        fills: &Fills<'_>,
        transport: &Transport<'_>,
    ) {
        a.arrived();
        let contract = Contract {
            not_run: &op.not_run,
            problem: &self.header.profile.problem,
        };
        match judge(record, contract, fills, transport) {
            Ok(j) => {
                let seen = j.seen();
                a.ended(j.judgement, seen);
            }
            Err(e) => a.fault("rest.predicate", e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hyper::client::conn::http1;

    use super::{Conn, Outgoing, Place, Pool, place};
    use crate::clock::RealClock;
    use crate::io::{Io, Probe, Sockets};
    use crate::stamps::{Feeds, Stamps};

    /// An idle connection over a pipe, its place one of `sockets`'; its task
    /// spawned where tasks are.
    async fn idle(stamps: &Stamps<RealClock>, sockets: &Arc<Sockets>, since: u64) -> Conn {
        let (client, _server) = tokio::io::duplex(64);
        let slot = sockets.take().unwrap();
        let held = slot.held();
        let probe = Arc::new(Probe::default());
        let (sender, connection) =
            http1::handshake::<_, Outgoing>(Io::new(client, probe.clone(), slot))
                .await
                .unwrap();
        let task = stamps.spawn_on(
            &tokio::runtime::Handle::current(),
            Feeds::Nothing,
            async move {
                let _ = connection.await;
            },
        );
        Conn {
            sender,
            probe,
            idle_since: since,
            task,
            _held: held,
        }
    }

    /// At the cap (decision 19): a dying socket awaited, the idle kept; none
    /// dying, the idle evicted; none idle, full.
    #[test]
    fn a_place_at_the_cap() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let stamps = Stamps::new(
                Arc::new(RealClock::start(1_000_000).unwrap()),
                ascetic_ddd_command_log::fills::Namespace(1),
            );
            let sockets = Sockets::new(2);
            let pool = Pool::default();
            pool.put("gateway", idle(&stamps, &sockets, 5).await);
            // A second socket, its connection gone, its own still open.
            let dying = sockets.take().unwrap();
            drop(dying.held());
            assert!(matches!(place(&sockets, &pool), Place::Dying));
            assert!(pool.lock().get("gateway").is_some_and(|c| c.len() == 1));
            // Dropped: under the cap again.
            drop(dying);
            let taken = place(&sockets, &pool);
            assert!(matches!(taken, Place::Taken(_)));
            // At the cap, none dying: the idle evicted.
            assert!(matches!(place(&sockets, &pool), Place::Dying));
            assert!(pool.lock().get("gateway").is_some_and(Vec::is_empty));
            // Its task aborted, its socket drops: a place, then none left.
            tokio::task::yield_now().await;
            let held = place(&sockets, &pool);
            assert!(matches!(held, Place::Taken(_)));
            assert!(matches!(place(&sockets, &pool), Place::Full));
            drop((taken, held));
        });
    }
}
