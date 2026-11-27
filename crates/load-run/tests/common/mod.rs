//! What the shell's tests share: `sim`'s scripted handler as a carrier.

use std::future::Future;
use std::sync::Arc;

use ascetic_ddd_command_log::record::{Header, Record};
use ascetic_ddd_load_run::carrier::Carrier;
use ascetic_ddd_load_run::clock::Clock;
use ascetic_ddd_load_run::stamps::Attempt;
use ascetic_ddd_scheduler::machine::Seen;
use ascetic_ddd_scheduler::sim;

pub type Script = dyn Fn(u64, u32, &Record) -> sim::Attempt + Send + Sync;

/// `sim`'s scripted handler as a carrier: the same instants, the same
/// judgements, reported through the attempt — each judged in `judging` µs of
/// its clock, a floor held meanwhile (`sim` judges at once).
pub struct Scripted {
    pub script: Arc<Script>,
    pub judging: Arc<dyn Fn(u64, u32) -> u64 + Send + Sync>,
}

impl Scripted {
    pub fn new(script: Arc<Script>) -> Scripted {
        Scripted {
            script,
            judging: Arc::new(|_, _| 0),
        }
    }
}

impl<C: Clock> Carrier<C> for Scripted {
    fn carries(&self, _: &Header) -> Result<(), String> {
        Ok(())
    }

    fn needs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::Needs, String> {
        Ok(ascetic_ddd_load_run::carrier::Needs::Nothing)
    }

    fn source_costs(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
        _: u32,
    ) -> Result<ascetic_ddd_load_run::carrier::SourceCosts, String> {
        Ok(ascetic_ddd_load_run::carrier::SourceCosts::NOTHING)
    }

    fn attempts(
        &self,
        _: &ascetic_ddd_command_log::validate::Peaks,
    ) -> Result<ascetic_ddd_load_run::carrier::AttemptCosts, ascetic_ddd_load_run::carrier::Unstated>
    {
        Ok(ascetic_ddd_load_run::carrier::AttemptCosts::NOTHING)
    }

    fn gate_bytes(&self) -> ascetic_ddd_load_run::stand::GateBytes {
        ascetic_ddd_load_run::stand::GateBytes::NONE
    }

    fn probe(
        &self,
        _: u32,
        _: std::sync::Arc<C>,
        _: u64,
    ) -> impl std::future::Future<Output = Vec<ascetic_ddd_load_run::carrier::Unprobed>> + Send + 'static
    {
        std::future::ready(Vec::new())
    }

    fn carry(&self, mut a: Attempt<C>) -> impl Future<Output = ()> + Send + 'static {
        let script = self.script.clone();
        let judging = self.judging.clone();
        async move {
            let s = script(a.line(), a.attempt(), a.record());
            let judging = judging(a.line(), a.attempt());
            let sent = a.dispatched().saturating_add(s.sent_after);
            // An attempt whose bytes never left is answered by nobody: past
            // its deadline, its closing report, as `sim` draws it (decision
            // 19).
            if sent >= a.deadline() {
                a.clock().sleep_until(a.deadline().saturating_add(1)).await;
                let report = sim::drawn_unsent(a.line(), a.attempt(), a.dispatched());
                a.left(report);
                return;
            }
            a.clock().sleep_until(sent).await;
            a.sent();
            match s.answer_after.map(|d| sent.saturating_add(d)) {
                Some(t) if t <= a.deadline() => {
                    a.clock().sleep_until(t).await;
                    a.arrived();
                    if judging > 0 {
                        let until = a.clock().now().saturating_add(judging);
                        a.clock().sleep_until(until).await;
                    }
                    let seen = Seen {
                        after: s.after,
                        ..Seen::default()
                    };
                    a.ended(s.judgement, seen);
                }
                _ => a.unanswered(),
            }
        }
    }
}
