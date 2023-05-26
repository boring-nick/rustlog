use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Migrate existing justlog logs
    Migrate {
        /// The justlog logs folder
        #[clap(short, long, value_parser)]
        source_dir: String,
        /// List of channel ids to migrate (None specified = migrate all)
        #[clap(short, long, value_parser)]
        channel_id: Vec<String>,
    },
}
