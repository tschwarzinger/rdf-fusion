use std::fs;
use std::path::Path;

#[allow(dead_code)]
pub(crate) fn load_queries(
    dir: impl AsRef<Path>,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut queries = Vec::new();
    let dir_path = dir.as_ref();
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "sparql") {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            let content = fs::read_to_string(&path)?;
            queries.push((name, content));
        }
    }
    queries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(queries)
}
