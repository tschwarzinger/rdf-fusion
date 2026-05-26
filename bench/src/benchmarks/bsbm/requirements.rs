use crate::benchmarks::bsbm::NumProducts;
use crate::environment::BenchmarkContext;
use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use anyhow::bail;
use reqwest::Url;
use std::fs::File;
use std::path::PathBuf;

/// Downloads the BSBM tools from a GitHub fork.
pub fn download_bsbm_tools(target_dir: PathBuf) -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://github.com/Tpt/bsbm-tools/archive/59d0a8a605b26f21506789fa1a713beb5abf1cab.zip")
            .expect("parse dataset-name"),
        file_name: target_dir,
        action: Some(FileAction::Unpack(ArchiveType::Zip)),
    }
}

/// Calls the BSBM tools to generate the dataset.
pub fn generate_dataset_requirement(
    workdir: PathBuf,
    dataset_path: PathBuf,
    td_data_dir: PathBuf,
    num_products: NumProducts,
) -> PrepRequirement {
    let file_name_str = dataset_path.display().to_string();
    PrepRequirement::RunCommand {
        workdir,
        program: "./generate".to_owned(),
        args: vec![
            "-fc".to_owned(), // We do not support RDFS reasoning
            "-pc".to_owned(), // Product Count
            format!("{}", num_products),
            "-dir".to_owned(),
            td_data_dir.display().to_string(),
            "-fn".to_owned(),
            format!("{}", &file_name_str[..file_name_str.len() - 3]), // The script appends .nt
        ],
        check_requirement: Box::new(move |_ctx: &BenchmarkContext| {
            if File::open(&dataset_path).is_err() {
                bail!("File {} does not exist", dataset_path.display());
            }
            Ok(())
        }),
    }
}

/// Copies the pre-generated queries from Oxigraph.
pub fn copy_pre_generated_queries(
    source_path: PathBuf,
    target_path: PathBuf,
) -> PrepRequirement {
    PrepRequirement::CopyFile {
        source_path,
        target_path,
        action: Some(FileAction::Unpack(ArchiveType::Bz2)),
    }
}
