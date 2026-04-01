use anyhow::Result;
use rdf_fusion::model::{Dataset, NamedNode};
use std::fmt::Write;
use text_diff::{Difference, diff};
use time::OffsetDateTime;

#[derive(Debug)]
pub struct TestResult {
    pub test: NamedNode,
    pub outcome: Result<()>,
    pub date: OffsetDateTime,
}

pub(crate) fn dataset_diff(expected: &Dataset, actual: &Dataset) -> String {
    format_diff(
        &normalize_dataset_text(expected),
        &normalize_dataset_text(actual),
        "quads",
    )
}

fn normalize_dataset_text(store: &Dataset) -> String {
    let mut quads: Vec<_> = store.iter().map(|q| q.to_string()).collect();
    quads.sort();
    quads.join("\n")
}

pub(crate) fn format_diff(expected: &str, actual: &str, kind: &str) -> String {
    let (_, changeset) = diff(expected, actual, "\n");
    let mut ret = String::new();
    writeln!(
        &mut ret,
        "Note: missing {kind} in yellow and extra {kind} in blue"
    )
    .unwrap();
    for seq in changeset {
        match seq {
            Difference::Same(x) => {
                writeln!(&mut ret, "{x}").unwrap();
            }
            Difference::Add(x) => {
                writeln!(&mut ret, "\x1B[94m{x}\x1B[0m").unwrap();
            }
            Difference::Rem(x) => {
                writeln!(&mut ret, "\x1B[93m{x}\x1B[0m").unwrap();
            }
        }
    }
    ret
}
