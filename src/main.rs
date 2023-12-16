mod flows;
mod repo;
mod retention_plan;
mod runner;

// use flows::DeduplicationOptions;
use repo::Repo;
use retention_plan::RetentionPlan;
use runner::Runner;
use std::{path::PathBuf, process};

use clap::{Args, Parser, Subcommand};

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

    let runner = Runner { verbose: true };

    match &cli.command {
        Commands::Backup(args) => {
            let backup_opts = flows::BackupOptions {
                source_paths: &args.source_paths,
                backup_path: &args.backup_path.backup_path,
                archive_mode: false,
            };
            let repo = Repo::from(&backup_opts.backup_path, args.initialize)?;

            return flows::run_backup_flow(&repo, &backup_opts);
            // return flows::run_deduplication_flow(
            //     &Repo::existing(&backup_opts.backup_path)?,
            //     &DeduplicationOptions {
            //         deep_compare: true,
            //         preserve_mtime: false,
            //     },
            //     &runner,
            // );
        }
        Commands::Cleanup(args) => {
            let cleanup_opts = flows::CleanupOptions {
                backup_path: &args.backup_path.backup_path,
                retention_plan: &args.retention_plan.retention_plan,
                force_delete: args.force_delete,
            };
            let repo = Repo::existing(&cleanup_opts.backup_path)?;

            return flows::run_cleanup_flow(repo, cleanup_opts, &runner);
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
