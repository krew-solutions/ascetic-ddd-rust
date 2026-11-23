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
use std::pin::Pin;
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
use crate::io::{Io, Probe};
use crate::stamps::Attempt;
use crate::stand::{Egress, Route, Stand};

/// What every frame of a run is filled from beside its step.
pub struct Config {
    /// The run's namespace.
    pub namespace: Namespace,
    /// The run's secret.
    pub secret: RunSecret,
    /// T0's wall instant, microseconds since the Unix epoch.
    pub run_start: i64,
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

    /// The latest idle connection to `endpoint` its peer has not closed.
    fn checkout(&self, endpoint: &str) -> Option<Conn> {
        let mut idle = self.lock();
        let conns = idle.get_mut(endpoint)?;
        while let Some(c) = conns.pop() {
            if !c.sender.is_closed() {
                return Some(c);
            }
        }
        None
    }

    fn checkin(&self, endpoint: &str, conn: Conn) {
        self.lock()
            .entry(endpoint.to_owned())
            .or_default()
            .push(conn);
    }
}

struct Inner<S, E> {
    header: Arc<Header>,
    stand: S,
    egress: E,
    config: Config,
    /// By source.
    pools: Vec<Pool>,
}

/// The REST carrier: see the [module documentation](self).
pub struct RestCarrier<S, E> {
    inner: Arc<Inner<S, E>>,
}

impl<S: Stand, E: Egress> RestCarrier<S, E> {
    /// Carries the calls of the log `header` heads to `stand`'s routes,
    /// connecting by `egress`.
    pub fn new(header: Arc<Header>, stand: S, egress: E, config: Config) -> RestCarrier<S, E> {
        let sources = usize::try_from(header.profile.sources.max(1)).unwrap_or(1);
        let pools = (0..sources).map(|_| Pool::default()).collect();
        RestCarrier {
            inner: Arc::new(Inner {
                header,
                stand,
                egress,
                config,
                pools,
            }),
        }
    }
}

impl<C: Clock, S: Stand, E: Egress> Carrier<C> for RestCarrier<S, E> {
    fn carries(&self, header: &Header) -> Result<(), String> {
        if !header.channels.is_empty() {
            return Err("a log with channels: the bus is K3's".to_owned());
        }
        if !header.calls.is_empty() {
            return Err("a log with host calls: they are K3's".to_owned());
        }
        Ok(())
    }

    fn carry(&self, attempt: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let inner = self.inner.clone();
        async move { inner.attempt(attempt).await }
    }
}

/// The request of `frame` to `route`, its correlation header set.
fn build(
    frame: &Frame,
    route: &Route,
    correlation: (&str, &str),
) -> Result<http::Request<Outgoing>, http::Error> {
    let mut b = http::Request::builder()
        .method(frame.method.as_str())
        .uri(frame.target.as_str())
        .header(http::header::HOST, route.authority.as_str());
    for (name, value) in &frame.headers {
        b = b.header(name.as_str(), value.as_str());
    }
    b = b.header(correlation.0, correlation.1);
    b.body(Outgoing {
        data: frame.body.clone().map(Bytes::from),
    })
}

impl<S: Stand, E: Egress> Inner<S, E> {
    async fn connect(&self, source: u32, route: &Route) -> io::Result<Conn> {
        let stream = self.egress.connect(source, route).await?;
        let probe = Arc::new(Probe::default());
        let (sender, connection) = http1::handshake(Io::new(stream, probe.clone()))
            .await
            .map_err(io::Error::other)?;
        // The connection's own task, on the source's runtime: the attempt's.
        drop(tokio::spawn(async move {
            let _ = connection.await;
        }));
        Ok(Conn { sender, probe })
    }

    async fn attempt<C: Clock>(&self, mut a: Attempt<C>) {
        let record = a.shared_record();
        let Some(rest) = request::rest_of(&record.action) else {
            return a.fault("not a REST call".to_owned());
        };
        let header = &self.header;
        let Some(op) = header.operations.iter().find(|o| o.name == rest.op) else {
            return a.fault(format!("no operation {}", rest.op));
        };
        let Some(pool) = usize::try_from(rest.source)
            .ok()
            .and_then(|s| self.pools.get(s))
        else {
            return a.fault(format!("no source {}", rest.source));
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
            Err(e) => return a.fault(format!("its frame: {e}")),
        };
        let route = match self.stand.route(&record, rest) {
            Ok(r) => r,
            Err(why) => return a.fault(format!("its route: {why}")),
        };
        let id = render::correlation(self.config.namespace, &record.step, a.attempt());
        let deadline = a.deadline();
        // Until a byte of it leaves: a connection, the request handed to it.
        let (response, conn) = loop {
            if a.clock().now() >= deadline {
                return a.unanswered();
            }
            let mut conn = match pool.checkout(&route.endpoint) {
                Some(c) => c,
                None => match self.connect(rest.source, &route).await {
                    Ok(c) => c,
                    // Refused connects and their floor are K2b-6's; until
                    // then the frontier censors it, unsent.
                    Err(_) => return a.unanswered(),
                },
            };
            let ready = tokio::select! {
                biased;
                r = poll_fn(|cx| conn.sender.poll_ready(cx)) => r.is_ok(),
                () = a.clock().sleep_until(deadline) => return a.unanswered(),
            };
            if !ready {
                continue;
            }
            let req = match build(&frame, &route, (&header.profile.correlation, &id)) {
                Ok(r) => r,
                Err(e) => return a.fault(format!("its request: {e}")),
            };
            // Armed and handed over in this one poll: no flush between.
            conn.probe.arm(Box::new(a.sent_hook()));
            let sent = conn.sender.try_send_request(req);
            let outcome = tokio::select! {
                biased;
                r = sent => r,
                // Censored by the frontier at the deadline; the connection
                // dropped, never pooled (keeping it is K2b-8's).
                () = a.clock().sleep_until(deadline) => return a.unanswered(),
            };
            match outcome {
                Ok(response) => break (response, conn),
                Err(mut e) => {
                    // Handed back, or no byte of it accepted: no end.
                    if e.take_message().is_some() || conn.probe.written() == 0 {
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
                f = poll_fn(|cx| Pin::new(&mut incoming).poll_frame(cx)) => f,
                () = a.clock().sleep_until(deadline) => return a.unanswered(),
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
        self.judged(a, &record, op, &fills, &Transport::Answered(read));
        if whole && !conn.sender.is_closed() {
            pool.checkin(&route.endpoint, conn);
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
            Err(e) => a.fault(format!("its predicate: {e}")),
        }
    }
}
