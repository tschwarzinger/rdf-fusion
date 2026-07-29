use std::path::PathBuf;

/// Defines a requirement of preparing for a benchmark.
pub enum BenchRequirement {
    /// Requires that a file exists.
    FileExists(PathBuf),
    /// Requires that a directory exists.
    DirectoryExists(PathBuf),
}
