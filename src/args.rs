use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Rebuild user indexes
    Reindex {
        /// Comma-separated list of channels to reindex
        #[clap(short, long, value_parser)]
        channels: Option<String>,
    },
    /// Print index
    PrintIndex { file_path: PathBuf },
    /// Migrate existing justlog logs
    Migrate {
        /// The justlog logs folder
        #[clap(short, long, value_parser)]
        source_dir: String,
    },
}
