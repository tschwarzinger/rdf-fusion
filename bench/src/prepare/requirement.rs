use crate::environment::BenchmarkContext;
use crate::prepare::FileAction;
use reqwest::Url;
use std::path::PathBuf;

type PrepClosure = Box<dyn Fn(&BenchmarkContext) -> anyhow::Result<()> + Send>;

/// Defines a requirement of preparing for a benchmark.
pub enum PrepRequirement {
    /// Requires that a file is copied from a given (relative) path to a given (relative) path.
    CopyFile {
        /// The file path of the source file.
        source_path: PathBuf,
        /// The target path of the copied file.
        target_path: PathBuf,
        /// An optional action that is applied to the copied file.
        action: Option<FileAction>,
    },
    /// Requires that a file is downloaded at a given (relative) path.
    FileDownload {
        /// The URL that can be used to download the file.
        url: Url,
        /// The file name of the resulting file.
        file_name: PathBuf,
        /// An optional action that is applied to the downloaded file.
        action: Option<FileAction>,
    },
    /// Runs a closure.
    RunClosure {
        /// The closure to execute.
        execute: PrepClosure,
        /// A checking function that can be used to check if the requirement is fulfilled.
        check_requirement: PrepClosure,
    },
    /// Runs a command.
    RunCommand {
        /// The working directory.
        workdir: PathBuf,
        /// The program to run.
        program: String,
        /// The args for the program.
        args: Vec<String>,
        /// A checking function that can be used to check if the requirement is fulfilled.
        check_requirement: PrepClosure,
    },
}
