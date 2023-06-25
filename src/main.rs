// mod flows;
// mod repo;
mod retention_plan;
use clap::{Args, Parser, Subcommand};


// ---------- Shared Arguments -----------
#[derive(Args)]
struct ArgBackupPath {
    /// The path of a directory of the backups.
    #[arg(short, long)]
    backup_path: String,
}

#[derive(Args)]
struct ArgRetentionPlan {
    /// The path of a directory of the backups.
    #[arg(short, long)]
    backup_path: String,
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
    Verify(ScrubCommands),
}

#[derive(Args)]
struct BackupArgs {
    
    #[command(flatten)]
    backup_path: ArgBackupPath,

    #[arg(long, default_value_t = false)]
    initialize: bool,

    #[command(flatten)]
    retention_plan: ArgRetentinonPlan,

}

#[derive(Args)]
struct CleanupArgs {
    #[arg(short, long)]
    backup_path: String,

    /// Specifies a minimum number of backups to delete. 
    #[arg(short, long, default_value_t=0)]
    force_delete: u64


}

#[derive(Args)]
struct ScrubCommands {
    #[arg(short, long)]
    backup_path: String,
}

fn main() {
    let cli = Cli::parse();
    //     match &cli.command {
    //         Commands::Backup(args) =>
    //     }
}
