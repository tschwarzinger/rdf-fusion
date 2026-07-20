use crate::operation::SparqlRawOperation;
use std::fs;
use std::path::PathBuf;

#[allow(clippy::panic)]
#[allow(clippy::panic_in_result_fn)]
#[allow(clippy::expect_used)]
pub fn list_raw_operations<TQueryName: TryFrom<u8>>(
    path: PathBuf,
) -> anyhow::Result<impl Iterator<Item = SparqlRawOperation<TQueryName>>> {
    #[allow(clippy::disallowed_methods)]
    let reader = fs::read(path)?;
    let result = csv::Reader::from_reader(reader.as_slice())
        .records()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|record| {
            let query_id = record[0].parse::<u8>().expect("Can't parse query id");
            let Ok(query_name) = TQueryName::try_from(query_id) else {
                panic!("Invalid query id: {query_id}")
            };

            match &record[1] {
                "query" => SparqlRawOperation::Query(query_name, record[2].into()),
                _ => panic!("Unexpected operation kind {}", &record[1]),
            }
        });
    Ok(result)
}
