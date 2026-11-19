//! The golden tables (ADR-0045 decision 8): what this version of the crate
//! generates, row by row, in layers — keys, units, laws, combinators,
//! placements, a reference plan. The checked-in `golden/tables.txt` is the
//! algorithm's identity: [`ALGORITHM`](crate::ALGORITHM) is its digest, so a
//! changed value changes the version in the same edit, and a reader of a
//! scenario made by another version refuses it.

use crate::apportion::apportion;
use crate::generator::{
    Gen, GenExt, Refusal, apportioned, constant, draw, from_fn, replicate, skewed_count,
    weighted_count,
};
use crate::key::{Key, Namespace, Seed, Unit};
use crate::law::{
    Binomial, Categorical, Empirical, Exponential, IndexLaw, Law, LogNormal, Pareto, Partitioned,
    Poisson, Skew, Truncated, Uniform, UniformIndex, UniformInt, Weibull,
};
use crate::permutation::Permutation;
use crate::population::{Plan, Pool};

/// The checked-in tables.
pub(crate) const TABLES: &str = include_str!("../golden/tables.txt");

/// The units every law and index law is evaluated at.
fn units() -> Vec<Unit> {
    [
        0,
        1,
        Unit::MAX_RAW / 3,
        Unit::MAX_RAW / 2,
        Unit::MAX_RAW / 3 * 2,
        Unit::MAX_RAW - 1,
        Unit::MAX_RAW,
    ]
    .into_iter()
    .filter_map(Unit::from_raw)
    .collect()
}

fn hex(key: &Key) -> String {
    let (k0, k1) = key.words();
    format!("{k0:016x}{k1:016x}")
}

/// A float as its bits: no formatting of floats, which has changed between
/// Rust releases, enters the tables.
fn bits(x: f64) -> String {
    format!("{:016x}", x.to_bits())
}

fn continuous(rows: &mut Vec<String>, law: &dyn Law<Value = f64>) {
    let values: Vec<String> = units().into_iter().map(|u| bits(law.quantile(u))).collect();
    rows.push(format!("law {law}: {}", values.join(" ")));
}

fn discrete(rows: &mut Vec<String>, law: &dyn Law<Value = u64>) {
    let values: Vec<String> = units()
        .into_iter()
        .map(|u| law.quantile(u).to_string())
        .collect();
    rows.push(format!("law {law}: {}", values.join(" ")));
}

fn index(rows: &mut Vec<String>, law: &dyn IndexLaw) {
    for n in [1u64, 10, 1_000_000_000] {
        let values: Vec<String> = units()
            .into_iter()
            .map(|u| law.index(u, n).to_string())
            .collect();
        rows.push(format!("law {law} over {n}: {}", values.join(" ")));
    }
}

/// Every row, in order: `layer subject: values`.
pub(crate) fn rows() -> Result<Vec<String>, Refusal> {
    let mut rows = Vec::new();
    let root = || Key::root(Seed(42));

    // Keys.
    rows.push(format!("key root(42): {}", hex(&root())));
    for label in ["", "os", "management_mode"] {
        rows.push(format!(
            "key child({label:?}): {}",
            hex(&root().child(label))
        ));
    }
    for i in [0, 1, u64::MAX] {
        rows.push(format!("key at({i}): {}", hex(&root().at(i))));
    }
    rows.push(format!(
        "key part((7, \"ab\")): {}",
        hex(&root().part(&(7u64, "ab")))
    ));
    rows.push(format!(
        "key internal(\"os\"): {}",
        hex(&root().internal("os"))
    ));
    rows.push(format!(
        "key uuid under 7: {}",
        root().child("d").uuid(&Namespace(7))
    ));

    // Units.
    for u in units() {
        rows.push(format!("unit {}: {}", u.raw(), bits(u.value())));
    }

    // Laws: values, counts, indices.
    continuous(&mut rows, &Uniform::new(-3.0, 7.5)?);
    continuous(&mut rows, &Exponential::new(2.5)?);
    continuous(&mut rows, &Pareto::new(1.0, 1.16)?);
    continuous(&mut rows, &LogNormal::new(0.0, 1.0)?);
    continuous(&mut rows, &Weibull::new(2.0, 0.5)?);
    continuous(
        &mut rows,
        &Truncated::new(Pareto::new(1.0, 1.16)?, 1.0, 1000.0)?,
    );
    discrete(&mut rows, &UniformInt::new(3, 1_000_003)?);
    discrete(
        &mut rows,
        &Categorical::from_weights(&[0.7, 0.2, 0.0, 0.07, 0.03])?,
    );
    discrete(
        &mut rows,
        &Empirical::from_histogram(&[(1, 10), (5, 3), (40, 1)])?,
    );
    discrete(&mut rows, &Poisson::new(10_000.0)?);
    discrete(&mut rows, &Binomial::new(1_000_000, 0.01)?);
    index(&mut rows, &UniformIndex);
    index(&mut rows, &Skew::new(2.0)?);
    index(&mut rows, &Partitioned::new(&[0.7, 0.2, 0.07, 0.03])?);
    // `Truncated` over a discrete law: the same type, another instance.
    discrete(&mut rows, &Truncated::new(Poisson::new(50.0)?, 40, 70)?);

    // Combinators, at fixed keys.
    let member = |i: u64| root().child("member").at(i);
    let twelve = Poisson::new(12.0)?;
    let poisson = || draw(twelve.clone());
    let mut combinators = Vec::new();
    for i in 0..4 {
        let optional = poisson().optional(0.5)?;
        let replicated = replicate(poisson(), poisson(), 100);
        let chained =
            poisson().and_then("next", |n: &u64| constant(*n).map(|n| n.saturating_add(1)));
        let skewed = skewed_count(50.0, 2.0)?;
        let weighted = weighted_count(50.0, &[0.7, 0.2, 0.07, 0.03])?;
        let split = apportioned(1_000, Categorical::from_counts(&[3, 1, 2])?);
        let structure = from_fn(|k: Key| Ok((k.child("a").bits(), k.child("b").bits())));
        combinators.push(format!(
            "combinator member {i}: optional {:?} replicate {:?} and_then {:?} skewed {:?} weighted {:?} apportioned {:?} from_fn {:?}",
            optional.generate(member(i)).ok(),
            replicated.generate(member(i)).ok(),
            chained.generate(member(i)).ok(),
            skewed.generate(member(i)).ok(),
            weighted.generate(member(i)).ok(),
            split.generate(member(i)).ok().map(|a| a.counts().collect::<Vec<_>>()),
            structure.generate(member(i)).ok(),
        ));
    }
    rows.extend(combinators);

    // Placements.
    for n in [10u64, 1_000, (1 << 20) + 1] {
        let p = Permutation::new(n, root().child("arrival"));
        let values: Vec<String> = [0, 1, n / 2, n.saturating_sub(1)]
            .into_iter()
            .filter_map(|i| p.at(i))
            .map(|v| v.to_string())
            .collect();
        rows.push(format!("permutation of {n}: {}", values.join(" ")));
    }
    let split = apportion(
        1_000_003,
        Categorical::from_weights(&[0.5, 0.0, 0.25, 0.25])?,
        root().child("split").unit(),
    );
    rows.push(format!(
        "apportion: {:?}",
        split.counts().collect::<Vec<_>>()
    ));
    let pool = Pool::new(10.0, root().child("pool"))?;
    rows.push(format!(
        "pool of 10: before 0, 9, 10, 99 {} {} {} {}; parent 1, 5 arrives before {} {}",
        pool.parents_before(0),
        pool.parents_before(9),
        pool.parents_before(10),
        pool.parents_before(99),
        pool.arrives_before(1),
        pool.arrives_before(5),
    ));

    // A reference plan: two scopes of devices, picked groups, their digest.
    rows.push(format!("plan: {}", reference_plan()?));
    Ok(rows)
}

/// A small plan of every kind of step: its records, digested.
fn reference_plan() -> Result<String, Refusal> {
    let mut plan = Plan::new(Seed(7));
    let os = Categorical::from_weights(&[0.6, 0.3, 0.1])?;
    let groups = plan.population::<u64, _, _>(
        "group",
        draw(Poisson::new(20.0)?),
        from_fn(|k: Key| Ok(k.child("name").bits())),
    )?;
    let devices = plan.population::<u64, _, _>("device", constant(500u64), draw(os))?;
    let mut digest = Digest::default();
    for tenant in 0..2u64 {
        let (g, d) = (groups.scope(tenant)?, devices.scope(tenant)?);
        let skew = Skew::new(2.0)?;
        for i in 0..d.size() {
            let device = d.value(i)?;
            let group = g.pick(d.key_of(i).child("group"), &skew).map(|r| r.index);
            digest.add(&format!("{tenant} {i} {device} {group:?}"));
        }
    }
    Ok(digest.hex())
}

/// SipHash-2-4-128 with zero keys over lines: the tables' digest.
#[derive(Default)]
pub(crate) struct Digest(Vec<u8>);

impl Digest {
    pub(crate) fn add(&mut self, line: &str) {
        self.0.extend_from_slice(line.as_bytes());
        self.0.push(b'\n');
    }

    pub(crate) fn hex(&self) -> String {
        let (h1, h2) = crate::key::digest(&self.0);
        format!("{h1:016x}{h2:016x}")
    }
}

/// The digest of `text`.
#[cfg(test)]
pub(crate) fn digest_of(text: &str) -> String {
    let mut d = Digest::default();
    d.0.extend_from_slice(text.as_bytes());
    d.hex()
}

/// Where the computed tables first differ from the checked-in ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoldenMismatch {
    /// The layer: `key`, `unit`, `law`, `combinator`, `permutation`,
    /// `apportion`, `pool`, `plan`; or `count` if the number of rows differs.
    pub layer: String,
    /// The row expected.
    pub expected: String,
    /// The row computed.
    pub computed: String,
}

impl std::fmt::Display for GoldenMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the {} layer differs from the golden tables:\n  expected {}\n  computed {}",
            self.layer, self.expected, self.computed
        )
    }
}

impl std::error::Error for GoldenMismatch {}

/// Compares the computed rows with the checked-in ones, layer by layer.
pub(crate) fn check() -> Result<(), GoldenMismatch> {
    let computed = rows().map_err(|refusal| GoldenMismatch {
        layer: "refusal".to_owned(),
        expected: "rows".to_owned(),
        computed: refusal.to_string(),
    })?;
    compare(TABLES, &computed)
}

/// The first row where `tables` and `computed` differ, or their counts.
fn compare(tables: &str, computed: &[String]) -> Result<(), GoldenMismatch> {
    let expected: Vec<&str> = tables.lines().collect();
    for (e, c) in expected.iter().zip(computed) {
        if *e != c {
            return Err(GoldenMismatch {
                layer: e.split(' ').next().unwrap_or("").to_owned(),
                expected: (*e).to_owned(),
                computed: c.clone(),
            });
        }
    }
    if expected.len() != computed.len() {
        return Err(GoldenMismatch {
            layer: "count".to_owned(),
            expected: format!("{} rows", expected.len()),
            computed: format!("{} rows", computed.len()),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R2: the crate generates what its tables say, layer by layer; a
    /// difference names its layer and its first row.
    #[test]
    fn the_tables_are_the_crate_s() {
        if let Err(mismatch) = check() {
            panic!("{mismatch}");
        }
    }

    /// A table that lost a row, or gained one, is a mismatch of the count; a
    /// changed row names its layer.
    #[test]
    fn a_mismatch_names_what_differs() {
        let computed: Vec<String> = ["law a: 1", "plan: x"].map(String::from).to_vec();
        assert_eq!(compare("law a: 1\nplan: x\n", &computed), Ok(()));
        assert_eq!(compare("law a: 1\n", &computed).unwrap_err().layer, "count");
        assert_eq!(
            compare("law a: 1\nplan: x\nmore\n", &computed)
                .unwrap_err()
                .layer,
            "count"
        );
        let changed = compare("law a: 2\nplan: x\n", &computed).unwrap_err();
        assert_eq!(
            (changed.layer.as_str(), changed.computed.as_str()),
            ("law", "law a: 1")
        );
    }

    /// R2: the version is the tables' digest, so the tables cannot change
    /// without it.
    #[test]
    fn the_version_is_the_tables_digest() {
        assert_eq!(digest_of(TABLES), crate::ALGORITHM);
    }

    /// R2: every law type of the crate has rows. Counted from the source, so a
    /// new law fails this until the tables cover it.
    #[test]
    fn every_law_has_rows() {
        let sources = [
            include_str!("law/continuous.rs"),
            include_str!("law/discrete.rs"),
            include_str!("law/index.rs"),
            include_str!("law/truncated.rs"),
        ];
        let impls: usize = sources
            .iter()
            .map(|s| s.matches(" Law for ").count() + s.matches(" IndexLaw for ").count())
            .sum();
        let covered = [
            "uniform(",
            "exponential(",
            "pareto(",
            "lognormal(",
            "weibull(",
            "truncated(",
            "uniform_int(",
            "categorical[",
            "empirical[",
            "poisson(",
            "binomial(",
            "uniform_index",
            "skew(",
            "partitioned(",
        ];
        assert_eq!(
            impls,
            covered.len(),
            "a law without rows in the golden tables"
        );
        let rows = rows().unwrap();
        for prefix in covered {
            assert!(
                rows.iter().any(|r| r.starts_with(&format!("law {prefix}"))),
                "no rows for {prefix}"
            );
        }
    }

    /// Writes `golden/tables.txt` from the crate as it is. Run only on purpose,
    /// then set `ALGORITHM` to the digest the version test reports: a changed
    /// table is a new version.
    #[test]
    #[ignore = "regenerates golden/tables.txt"]
    fn regenerate() {
        let text: String = rows().unwrap().into_iter().map(|r| r + "\n").collect();
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/golden/tables.txt"),
            &text,
        )
        .unwrap();
        println!("ALGORITHM = {}", digest_of(&text));
    }
}
