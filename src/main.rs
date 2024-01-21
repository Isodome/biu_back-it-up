mod flows;
mod repo;
mod utils;

use libbiu::{
    run_backup_flow, run_cleanup_flow, BackupFlowOptions, CleanupFlowOptions, RetentionPlan,
};
use clap::{Args, Parser, Subcommand};
use std::{path::PathBuf, process};

// ---------- Shared Arguments -----------
#[derive(Args)]
struct ArgBackupPath {
    /// The path of a directory of the backups.
    #[arg(short, long)]
    backup_path: PathBuf,
}

#[derive(Args)]
struct ArgRetentionPlan {
    /// The path of a directory of the backups.
    #[arg(short, long,default_value_t = RetentionPlan::default()) ]
    retention_plan: RetentionPlan,
}

// ---------- CLI-----------
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Creates a new backup and cleans up old backups according to the retention plan.
    Backup(BackupArgs),
    // Deletes old backups based on the retention plan.
    Cleanup(CleanupArgs),
    // Verfies that the backups are intact.
    Verify(ScrubArgs),
}

#[derive(Args)]
struct BackupArgs {
    #[command(flatten)]
    backup_path: ArgBackupPath,

    /// By default the backup fails if the backup_path does not exist. If set the backup_path is created if missing.
    #[arg(long, default_value_t = false)]
    initialize: bool,

    #[command(flatten)]
    retention_plan: ArgRetentionPlan,

    /// A list of paths to the directories that we'll back up.
    #[arg(long, short, required = true)]
    source_paths: Vec<PathBuf>,
}

#[derive(Args)]
struct CleanupArgs {
    #[command(flatten)]
    backup_path: ArgBackupPath,

    #[command(flatten)]
    retention_plan: ArgRetentionPlan,

    /// Specifies a minimum number of backups to delete.
    #[arg(short, long, default_value_t = 0)]
    force_delete: i32,
}

#[derive(Args)]
struct ScrubArgs {
    #[command(flatten)]
    backup_path: ArgBackupPath,
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Backup(args) => {
            let opts = BackupFlowOptions {
                initialize: args.initialize,
                source_paths: args.source_paths.clone(),
                backup_path: args.backup_path.backup_path.clone(),
                follow_symlinks: false,
                deep_compare: true,
                preserve_mtime: false,
                min_bytes_for_dedup: 0,
            };
            run_backup_flow(opts)
        }
        Commands::Cleanup(args) => {
            let cleanup_opts = CleanupFlowOptions {
                backup_path: &args.backup_path.backup_path,
                retention_plan: &args.retention_plan.retention_plan,
                force_delete: args.force_delete,
            };
            run_cleanup_flow(cleanup_opts)
        }
        _ => panic!("Unkown command"),
    }
}

fn main() {
    let status = run();
    if status.is_err() {
        println!("{}", status.unwrap_err());
        process::exit(1);
    }
}
