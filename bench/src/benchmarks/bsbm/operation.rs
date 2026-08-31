use crate::operation::SparqlOperation;
use std::fs;
use std::path::PathBuf;

#[allow(clippy::panic)]
#[allow(clippy::panic_in_result_fn)]
#[allow(clippy::expect_used)]
pub fn list_operations(
    path: PathBuf,
) -> anyhow::Result<impl Iterator<Item = SparqlOperation>> {
    #[allow(clippy::disallowed_methods)]
    let reader = fs::read(path)?;
    let result = csv::Reader::from_reader(reader.as_slice())
        .records()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|record| {
            let query_name = ["Q", &record[0]].concat();
            match &record[1] {
                "query" => SparqlOperation::new(query_name, record[2].replace(" #", "")),
                _ => panic!("Unexpected operation kind {}", &record[1]),
            }
        });
    Ok(result)
}
