use crate::environment::BenchmarkContext;
use crate::prepare::actions::{FileAction, execute_file_action};
use anyhow::{Context, bail};
use std::path::Path;
use std::{fs, path};

pub fn ensure_copy_file(env: &BenchmarkContext, file_name: &Path) -> anyhow::Result<()> {
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
pub fn prepare_copy_file(
    env: &BenchmarkContext<'_>,
    source_path: &Path,
    target_path: &Path,
    action: Option<&FileAction>,
) -> anyhow::Result<()> {
    println!(
        "Copying file '{}' to '{}' ...",
        source_path.display(),
        target_path.display()
    );

    let target_path = env
        .join_data_dir(target_path)
        .context("Cant join data dir with target path")?;
    if target_path.exists() {
        if target_path.is_dir() {
            fs::remove_dir_all(&target_path)
                .context("Cannot remove existing directory in prepare_copy_file")?;
        } else {
            fs::remove_file(&target_path)
                .context("Cannot remove existing file in prepare_copy_file")?;
        }
    }

    fs::copy(source_path, &target_path).context("Cannot copy file")?;
    println!("File Copied.");

    execute_file_action(&target_path, action)?;

    Ok(())
}
