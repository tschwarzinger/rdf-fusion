use crate::environment::BenchmarkContext;
use crate::prepare::actions::{FileAction, execute_file_action};
use anyhow::{Context, bail};
use reqwest::Url;
use std::path::{Path, PathBuf};
use std::{fs, path};

pub fn ensure_file_download(
    env: &BenchmarkContext,
    file_name: &Path,
) -> anyhow::Result<()> {
    let file_path = env.parent().join_data_dir(file_name)?;
    if !file_path.exists() {
        bail!(
            "{:?} does not exist ({:?})",
            &file_path,
            &path::absolute(&file_path)
        );
    }
    Ok(())
}

/// Downloads a file from the given url and executes a possible `action` afterward
/// (e.g., Extract Archive).
pub async fn prepare_file_download(
    env: &BenchmarkContext<'_>,
    url: Url,
    file_name: PathBuf,
    action: Option<FileAction>,
) -> anyhow::Result<()> {
    println!("Downloading file '{url}' ...");
    let file_path = env
        .join_data_dir(&file_name)
        .context("Cant join data dir with file name")?;
    if file_path.exists() {
        if file_path.is_dir() {
            fs::remove_dir_all(&file_path)
                .context("Cannot remove existing directory in prepare_file_download")?;
        } else {
            fs::remove_file(&file_path)
                .context("Cannot remove existing file in prepare_file_download")?;
        }
    }

    let response = reqwest::Client::new()
        .get(url.clone())
        .send()
        .await
        .with_context(|| format!("Could not send request to download file '{url}'"))?;
    if !response.status().is_success() {
        bail!(
            "Response code for file '{url}' was not OK. Actual: {}",
            response.status()
        )
    }

    let parent_file = file_path.parent().context("Cannot create parent dir")?;
    fs::create_dir_all(parent_file).context("Cannot create parent dir for file")?;
    fs::write(&file_path, &response.bytes().await?)
        .context("Can't write response to file")?;
    println!("File downloaded.");

    execute_file_action(&file_path, action.as_ref())?;

    Ok(())
}
