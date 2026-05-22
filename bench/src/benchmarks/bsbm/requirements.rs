use crate::benchmarks::bsbm::NumProducts;
use crate::environment::BenchmarkContext;
use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use anyhow::bail;
use reqwest::Url;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Downloads the BSBM tools from a GitHub fork.
pub fn download_bsbm_tools() -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://github.com/Tpt/bsbm-tools/archive/59d0a8a605b26f21506789fa1a713beb5abf1cab.zip")
            .expect("parse dataset-name"),
        file_name: PathBuf::from("bsbmtools"),
        action: Some(FileAction::Unpack(ArchiveType::Zip)),
    }
}

/// Calls the BSBM tools to generate the dataset.
pub fn generate_dataset_requirement(
    file_name: PathBuf,
    num_products: NumProducts,
) -> PrepRequirement {
    let file_name_str = file_name.display().to_string();
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("./bsbmtools"),
        program: "./generate".to_owned(),
        args: vec![
            "-fc".to_owned(), // We do not support RDFS reasoning
            "-pc".to_owned(), // Product Count
            format!("{}", num_products),
            "-dir".to_owned(),
            "../td_data".to_owned(),
            "-fn".to_owned(),
            format!("../{}", &file_name_str[..file_name_str.len() - 3]), // The script appends .nt
        ],
        check_requirement: Box::new(move |ctx: &BenchmarkContext| {
            let path = ctx.parent().join_data_dir(&file_name)?;
            if File::open(&path).is_err() {
                bail!("File {} does not exist", path.display());
            }
            Ok(())
        }),
    }
}

/// Copies the pre-generated queries from Oxigraph.
pub fn copy_pre_generated_queries(
    data_files_path: &Path,
    use_case: &str,
    target_path: PathBuf,
    num_products: NumProducts,
) -> PrepRequirement {
    let source_path = data_files_path
        .join("bsbm_queries")
        .join(format!("{use_case}-{num_products}.csv.bz2"));
    PrepRequirement::CopyFile {
        source_path,
        target_path,
        action: Some(FileAction::Unpack(ArchiveType::Bz2)),
    }
}
