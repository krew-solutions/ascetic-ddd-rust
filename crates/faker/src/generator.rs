//! Generators: pure arrows from a key to a value (ADR-0045 decision 5).
//!
//! A [`Gen`] takes `&self` and a [`Key`], so one generator serves every thread
//! at once, and has nothing to share. Python's FP factories become:
//!
//! | Python | Here |
//! |---|---|
//! | `ValueFactory` | [`constant`], [`draw`], [`from_fn`] |
//! | `StructureFactory` | [`from_fn`] with a struct literal, a `key.child("field")` per field |
//! | `ModeledFactory` | [`GenExt::map`], [`GenExt::try_map`] |
//! | `Pipe` | [`GenExt::and_then`] |
//! | `ReplicatedFactory` | [`replicate`] |
//! | `NullableDistributor` | [`GenExt::optional`] |
//! | o2m `SkewDistributor`, `WeightedDistributor` | [`skewed_count`], [`weighted_count`] |
//!
//! # The key each combinator passes on
//!
//! Two draws from one key are one draw: their values move together. Every
//! combinator therefore states where its own draws come from and which key its
//! parts receive, and the two never meet:
//!
//! | Combinator | Draws from | Passes on |
//! |---|---|---|
//! | `constant` | nothing | — |
//! | `draw(law)` | the key | — |
//! | `map`, `try_map` | nothing | the key |
//! | `and_then(label, f)` | nothing | the key to the first, `child(label)` to the second |
//! | `optional(p)` | an internal `present` | the key |
//! | `replicate(count, item, cap)` | nothing | an internal `count` to the count, `at(i)` to item `i` |
//!
//! So wrapping a generator in `optional` or `map` leaves the values it gives
//! unchanged: what it adds draws from a key the generator never sees. Internal
//! derivations sit under a tag a host's label cannot reach.

use std::fmt;
use std::marker::PhantomData;

use crate::apportion::{Apportionment, apportion};
use crate::key::Key;
use crate::law::{Categorical, InvalidLaw, Law};

/// A generated value refused: a smart constructor said no, a count passed its
/// cap. Generation aborts on it (ADR-0045 decision 6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    /// What was refused, and why.
    pub reason: String,
    /// The labels from the outermost generator in, as the refusal passed
    /// through `and_then`.
    pub path: Vec<&'static str>,
}

impl Refusal {
    /// A refusal for `reason`.
    pub fn new(reason: impl fmt::Display) -> Refusal {
        Refusal {
            reason: reason.to_string(),
            path: Vec::new(),
        }
    }

    fn within(mut self, label: &'static str) -> Refusal {
        self.path.insert(0, label);
        self
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.reason)
        } else {
            write!(f, "{}: {}", self.path.join("."), self.reason)
        }
    }
}

impl std::error::Error for Refusal {}

impl From<InvalidLaw> for Refusal {
    fn from(e: InvalidLaw) -> Refusal {
        Refusal::new(e)
    }
}

/// A pure arrow from a key to a value.
pub trait Gen: Send + Sync {
    /// What it gives.
    type Output;

    /// The value of `key`.
    fn generate(&self, key: Key) -> Result<Self::Output, Refusal>;
}

impl<G: Gen + ?Sized> Gen for &G {
    type Output = G::Output;

    fn generate(&self, key: Key) -> Result<G::Output, Refusal> {
        (**self).generate(key)
    }
}

impl<G: Gen + ?Sized> Gen for Box<G> {
    type Output = G::Output;

    fn generate(&self, key: Key) -> Result<G::Output, Refusal> {
        (**self).generate(key)
    }
}

/// Always `value`; draws nothing.
pub fn constant<T: Clone + Send + Sync>(value: T) -> Constant<T> {
    Constant(value)
}

/// See [`constant`].
#[derive(Clone, Debug)]
pub struct Constant<T>(T);

impl<T: Clone + Send + Sync> Gen for Constant<T> {
    type Output = T;

    fn generate(&self, _key: Key) -> Result<T, Refusal> {
        Ok(self.0.clone())
    }
}

/// A draw from `law`, from the key's unit.
pub fn draw<L: Law + Send + Sync>(law: L) -> Draw<L> {
    Draw(law)
}

/// See [`draw`].
#[derive(Clone, Debug)]
pub struct Draw<L>(L);

impl<L: Law + Send + Sync> Gen for Draw<L> {
    type Output = L::Value;

    fn generate(&self, key: Key) -> Result<L::Value, Refusal> {
        Ok(self.0.quantile(key.unit()))
    }
}

/// A generator written as a function of the key: the main idiom for a
/// struct, a `key.child("field")` per field.
///
/// ```
/// use ascetic_ddd_faker::generator::{Gen, draw, from_fn};
/// use ascetic_ddd_faker::law::{Categorical, Poisson};
/// use ascetic_ddd_faker::{Key, Seed};
///
/// struct Device { os: u64, apps: u64 }
///
/// let os = draw(Categorical::from_weights(&[0.6, 0.3, 0.1]).unwrap());
/// let apps = draw(Poisson::new(40.0).unwrap());
/// let device = from_fn(move |key: Key| {
///     Ok(Device {
///         os: os.generate(key.child("os"))?,
///         apps: apps.generate(key.child("apps"))?,
///     })
/// });
/// let d = device.generate(Key::root(Seed(1)).child("device").at(7)).unwrap();
/// # assert!(d.os < 3 && d.apps < 200);
/// ```
pub fn from_fn<T, F>(f: F) -> FromFn<F>
where
    F: Fn(Key) -> Result<T, Refusal> + Send + Sync,
{
    FromFn(f)
}

/// See [`from_fn`].
#[derive(Clone)]
pub struct FromFn<F>(F);

impl<T, F> Gen for FromFn<F>
where
    F: Fn(Key) -> Result<T, Refusal> + Send + Sync,
{
    type Output = T;

    fn generate(&self, key: Key) -> Result<T, Refusal> {
        (self.0)(key)
    }
}

/// The combinators every generator has.
pub trait GenExt: Gen + Sized {
    /// The value turned by `f`; the key passed on unchanged.
    fn map<U, F>(self, f: F) -> Map<Self, F>
    where
        F: Fn(Self::Output) -> U + Send + Sync,
    {
        Map(self, f)
    }

    /// The value turned by `f`, which may refuse it — a smart constructor.
    fn try_map<U, E, F>(self, f: F) -> TryMap<Self, F, E>
    where
        F: Fn(Self::Output) -> Result<U, E> + Send + Sync,
        E: fmt::Display,
    {
        TryMap(self, f, PhantomData)
    }

    /// A second generator chosen by the first's value, under `child(label)`:
    /// Python's `Pipe`, typed.
    fn and_then<H, F>(self, label: &'static str, f: F) -> AndThen<Self, F>
    where
        H: Gen,
        F: Fn(&Self::Output) -> H + Send + Sync,
    {
        AndThen(self, label, f)
    }

    /// `Some` with probability `present`, from a coin under an internal key; the
    /// value, when present, is the one the generator gives the key alone.
    fn optional(self, present: f64) -> Result<Optional<Self>, InvalidLaw> {
        if present.is_finite() && (0.0..=1.0).contains(&present) {
            Ok(Optional {
                inner: self,
                threshold: crate::law::threshold(present),
            })
        } else {
            crate::law::invalid("optional", "the probability must lie in [0, 1]")
        }
    }
}

impl<G: Gen> GenExt for G {}

/// See [`GenExt::map`].
#[derive(Clone)]
pub struct Map<G, F>(G, F);

impl<G, U, F> Gen for Map<G, F>
where
    G: Gen,
    F: Fn(G::Output) -> U + Send + Sync,
{
    type Output = U;

    fn generate(&self, key: Key) -> Result<U, Refusal> {
        self.0.generate(key).map(&self.1)
    }
}

/// See [`GenExt::try_map`].
pub struct TryMap<G, F, E>(G, F, PhantomData<fn() -> E>);

impl<G, U, E, F> Gen for TryMap<G, F, E>
where
    G: Gen,
    F: Fn(G::Output) -> Result<U, E> + Send + Sync,
    E: fmt::Display,
{
    type Output = U;

    fn generate(&self, key: Key) -> Result<U, Refusal> {
        (self.1)(self.0.generate(key)?).map_err(Refusal::new)
    }
}

/// See [`GenExt::and_then`].
#[derive(Clone)]
pub struct AndThen<G, F>(G, &'static str, F);

impl<G, H, F> Gen for AndThen<G, F>
where
    G: Gen,
    H: Gen,
    F: Fn(&G::Output) -> H + Send + Sync,
{
    type Output = H::Output;

    fn generate(&self, key: Key) -> Result<H::Output, Refusal> {
        let next = key.child(self.1);
        let first = self.0.generate(key)?;
        (self.2)(&first)
            .generate(next)
            .map_err(|r| r.within(self.1))
    }
}

/// See [`GenExt::optional`].
#[derive(Clone)]
pub struct Optional<G> {
    inner: G,
    /// `⌈present · 2⁵²⌉`: the coin is present when its `k` is below it.
    threshold: u64,
}

impl<G: Gen> Gen for Optional<G> {
    type Output = Option<G::Output>;

    fn generate(&self, key: Key) -> Result<Option<G::Output>, Refusal> {
        if key.internal("present").unit().raw() < self.threshold {
            self.inner.generate(key).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// `count` items from `item`, item `i` under `at(i)`; a count above `cap`
/// refused. The cap is not optional: a heavy tail must not decide how much
/// memory one value takes (ADR-0045 decision 4).
pub fn replicate<C, G>(count: C, item: G, cap: u64) -> Replicate<C, G>
where
    C: Gen<Output = u64>,
    G: Gen,
{
    Replicate { count, item, cap }
}

/// See [`replicate`].
#[derive(Clone)]
pub struct Replicate<C, G> {
    count: C,
    item: G,
    cap: u64,
}

impl<C, G> Gen for Replicate<C, G>
where
    C: Gen<Output = u64>,
    G: Gen,
{
    type Output = Vec<G::Output>;

    fn generate(&self, key: Key) -> Result<Vec<G::Output>, Refusal> {
        let n = self.count.generate(key.internal("count"))?;
        if n > self.cap {
            return Err(Refusal::new(format!(
                "a count of {n} is above its cap of {}",
                self.cap
            )));
        }
        (0..n).map(|i| self.item.generate(key.at(i))).collect()
    }
}

/// A fixed total split by weights, at the key's unit: [`apportion`] as a
/// generator, for a scope's counts.
pub fn apportioned(total: u64, weights: Categorical) -> Apportioned {
    Apportioned { total, weights }
}

/// See [`apportioned`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Apportioned {
    total: u64,
    weights: Categorical,
}

impl Gen for Apportioned {
    type Output = Apportionment;

    fn generate(&self, key: Key) -> Result<Apportionment, Refusal> {
        Ok(apportion(self.total, self.weights.clone(), key.unit()))
    }
}

/// How many children an owner has, skewed: Python's o2m `SkewDistributor`.
///
/// The owner's position `x` is uniform; its mean is `mean · (s + 1) · (1 − x)^s`,
/// whose average over `x` is `mean`; its count is Poisson with that mean. Two
/// draws, under two internal keys: a mixture is not a law of one unit.
///
/// The variance is `mean + mean² · ((s + 1)² / (2s + 1) − 1)`: about 45² at a
/// mean of 50 and `s = 2`, where a plain Poisson's is 50 (L7).
pub fn skewed_count(mean: f64, skew: f64) -> Result<SkewedCount, InvalidLaw> {
    if !(mean.is_finite() && mean >= 0.0) {
        return crate::law::invalid("skewed_count", "the mean must be finite and not negative");
    }
    if !(skew.is_finite() && skew >= 1.0) {
        return crate::law::invalid("skewed_count", "the skew must be finite and at least 1");
    }
    // The largest mean an owner can get, (s + 1) · mean, must be a Poisson's.
    crate::law::Poisson::new(mean * (skew + 1.0)).map(|_| SkewedCount { mean, skew })
}

/// See [`skewed_count`].
#[derive(Clone, Debug, PartialEq)]
pub struct SkewedCount {
    mean: f64,
    skew: f64,
}

impl Gen for SkewedCount {
    type Output = u64;

    fn generate(&self, key: Key) -> Result<u64, Refusal> {
        // (1 − x)^s with x uniform: the position's complement, exact.
        let rest = key.internal("position").unit().complement();
        let owner_mean = self.mean * (self.skew + 1.0) * crate::math::pow(rest, self.skew);
        let law = crate::law::Poisson::new(owner_mean)?;
        Ok(law.quantile(key.internal("count").unit()))
    }
}

/// How many children an owner has, by the weight of its part: Python's o2m
/// `WeightedDistributor`. The owners are cut into `m` equal parts by a uniform
/// position; an owner of part `j` has a Poisson count of mean
/// `mean · m · wⱼ / Σw`, so that the parts' shares of all children are the
/// weights and the average is `mean`.
///
/// Python also bent each part's mean towards its start; the bend went with the
/// m2o one (defect 3), and a measured shape is an `Empirical` law.
pub fn weighted_count(mean: f64, weights: &[f64]) -> Result<WeightedCount, InvalidLaw> {
    if !(mean.is_finite() && mean >= 0.0) {
        return crate::law::invalid("weighted_count", "the mean must be finite and not negative");
    }
    // Refused as a categorical's weights are: empty, negative, not finite, all zero.
    crate::law::Categorical::from_weights(weights)?;
    let total: f64 = weights.iter().sum();
    let m = weights.len() as f64;
    let means: Vec<f64> = weights.iter().map(|w| mean * m * w / total).collect();
    if let Some(top) = means.iter().copied().reduce(f64::max) {
        crate::law::Poisson::new(top)?;
    }
    Ok(WeightedCount { means })
}

/// See [`weighted_count`].
#[derive(Clone, Debug, PartialEq)]
pub struct WeightedCount {
    means: Vec<f64>,
}

impl Gen for WeightedCount {
    type Output = u64;

    fn generate(&self, key: Key) -> Result<u64, Refusal> {
        let parts = self.means.len() as u64;
        let part = crate::law::scale(key.internal("position").unit().raw(), parts);
        let mean = usize::try_from(part)
            .ok()
            .and_then(|p| self.means.get(p))
            .copied()
            .unwrap_or(0.0);
        let law = crate::law::Poisson::new(mean)?;
        Ok(law.quantile(key.internal("count").unit()))
    }
}
