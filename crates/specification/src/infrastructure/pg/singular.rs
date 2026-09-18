//! The singular of a collection's name, for the alias of its items:
//! `items` are each an `item_1`.
//!
//! The sources take it from an inflection library. An alias has to be
//! distinct, which its number sees to, and readable, which a few rules of
//! English do; nothing depends on the singular being right.

const UNCHANGED: [&str; 8] = [
    "series",
    "species",
    "news",
    "data",
    "equipment",
    "information",
    "money",
    "sheep",
];

const IRREGULAR: [(&str, &str); 6] = [
    ("people", "person"),
    ("children", "child"),
    ("men", "man"),
    ("women", "woman"),
    ("mice", "mouse"),
    ("feet", "foot"),
];

/// `word` is expected in lower case.
pub(super) fn singular(word: &str) -> String {
    if UNCHANGED.contains(&word) {
        return word.to_owned();
    }
    if let Some((_, one)) = IRREGULAR.iter().find(|(many, _)| *many == word) {
        return (*one).to_owned();
    }
    let strip = |suffix: &str| word.strip_suffix(suffix).filter(|stem| !stem.is_empty());
    let ends = |suffix: &&str| strip(suffix).is_some();
    if let Some(stem) = strip("ies") {
        return format!("{stem}y");
    }
    if ["sses", "shes", "ches", "xes", "zes", "uses"]
        .iter()
        .any(ends)
    {
        return strip("es").unwrap_or(word).to_owned();
    }
    match strip("s") {
        Some(stem) if !["s", "u", "i"].iter().any(|end| stem.ends_with(end)) => stem.to_owned(),
        _ => word.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::singular;

    #[test]
    fn the_names_the_sources_test_with() {
        for (many, one) in [
            ("items", "item"),
            ("categories", "category"),
            ("regions", "region"),
            ("tags", "tag"),
            ("series", "series"),
            ("addresses", "address"),
            ("boxes", "box"),
            ("statuses", "status"),
            ("children", "child"),
            ("status", "status"),
            ("item", "item"),
            ("s", "s"),
        ] {
            assert_eq!(singular(many), one, "{many}");
        }
    }
}
