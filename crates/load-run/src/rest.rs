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
use std::io;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use ascetic_ddd_command_log::answer::{self, Reply};
use ascetic_ddd_command_log::fills::{Fills, Namespace};
use ascetic_ddd_command_log::record::{Action, Header, Operation, Record};
use ascetic_ddd_command_log::render::{self, RunSecret};
use ascetic_ddd_command_log::request::{self, Frame};
use ascetic_ddd_scheduler::judge::{Contract, Transport, judge};
use bytes::Bytes;
use http_body::{Body, Frame as BodyFrame, SizeHint};
use hyper::client::conn::http1::{self, SendRequest};

use crate::carrier::Carrier;
use crate::clock::Clock;
use crate::decorate::{Decoration, Decorator, Undecorated};
use crate::io::{Io, Probe};
use crate::stamps::Attempt;
use crate::stand::{Egress, Route, Stand};
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

/// What every frame of a run is filled from beside its step.
pub struct Config {
    /// The run's namespace.
    pub namespace: Namespace,
    /// The run's secret.
    pub secret: RunSecret,
    /// T0's wall instant, microseconds since the Unix epoch.
    pub run_start: i64,
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
    /// The run's namespace, secret and wall instant of T0; the connect floor
    /// and the idle margin by default.
    pub fn new(namespace: Namespace, secret: RunSecret, run_start: i64) -> Config {
        Config {
            namespace,
            secret,
            run_start,
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
        RestCarrier {
            inner: Arc::new(Inner {
                header,
                stand,
                egress,
                decorator,
                config,
                pools,
            }),
        }
    }
}

impl<C: Clock, S: Stand, E: Egress, D: Decorator> Carrier<C> for RestCarrier<S, E, D> {
    fn carries(&self, header: &Header) -> Result<(), String> {
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
        Ok(())
    }

    fn carry(&self, attempt: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.clone();
        async move { inner.attempt(attempt).await }
    }
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
    ) -> io::Result<Conn> {
        let stream = self.egress.connect(source, route).await?;
        let probe = Arc::new(Probe::default());
        let (sender, connection) = http1::handshake(Io::new(stream, probe.clone()))
            .await
            .map_err(io::Error::other)?;
        // The connection's own task, on the source's runtime: the attempt's,
        // kept with the run's for a stop to abort (decision 18).
        a.spawn_connection(probe.clone(), async move {
            let _ = connection.await;
        });
        Ok(Conn {
            sender,
            probe,
            idle_since: 0,
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
        let Some(pool) = usize::try_from(rest.source)
            .ok()
            .and_then(|s| self.pools.get(s))
        else {
            return a.fault("rest.source", format!("no source {}", rest.source));
        };
        let captured = a.captured().to_vec();
        let wall = |at: u64| {
            self.config
                .run_start
                .saturating_add(i64::try_from(at).unwrap_or(i64::MAX))
        };
        let fills = Fills {
            namespace: self.config.namespace,
            secret: &self.config.secret,
            run_start: self.config.run_start,
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
        let id = render::correlation(self.config.namespace, &record.step, a.attempt());
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
            () = a.clock().sleep_until(past) => return a.unanswered(),
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
        a.handed();
        let deadline = a.deadline();
        // An answer stamped at the deadline is taken: the attempt leaves only
        // past it, and the frontier censors at it what had none.
        let mut looks = Looks {
            pre: Some(deadline.saturating_sub(header.profile.jitter / 2)),
            past,
        };
        let retire = header.profile.idle.saturating_sub(self.config.idle_margin);
        let mut connected_last: Option<u64> = None;
        // Until a byte of it leaves: a connection, the request handed to it.
        let (response, conn) = loop {
            if a.clock().now() >= deadline {
                return a.unanswered();
            }
            let mut conn = match pool.checkout(&route.endpoint, a.clock().now(), retire) {
                Some(c) => c,
                None => {
                    // Connects a floor apart, the first at once: a stand
                    // refusing them is not hammered, and the frontier censors
                    // an attempt never connected (decision 16).
                    if let Some(last) = connected_last {
                        let next = last.saturating_add(self.config.connect_floor);
                        if next >= deadline {
                            return a.unanswered();
                        }
                        a.clock().sleep_until(next).await;
                    }
                    connected_last = Some(a.clock().now());
                    let connecting = self.connect(&a, rest.source, &route);
                    match tokio::select! {
                        biased;
                        c = connecting => c,
                        () = a.clock().sleep_until(past) => return a.unanswered(),
                    } {
                        Ok(c) => c,
                        Err(_) => continue,
                    }
                }
            };
            let ready = tokio::select! {
                biased;
                r = poll_fn(|cx| conn.sender.poll_ready(cx)) => r.is_ok(),
                () = a.clock().sleep_until(past) => return a.unanswered(),
            };
            if !ready {
                continue;
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
            conn.probe.arm(Box::new(a.sent_hook()), a.path());
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
                        // connection dropped, never pooled (keeping it is
                        // K2b-8's).
                        if looks.done() {
                            return a.unanswered();
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
                                return a.unanswered();
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
