use crate::{repo::{Backup, Repo, RetentionPlan}, utils::Runner};
use chrono::prelude::*;
use std::path::Path;

#[derive(Debug)]
pub struct CleanupOptions<'a> {
    pub backup_path: &'a Path,
    pub retention_plan: &'a RetentionPlan,
    pub force_delete: i32,
}

struct MarkedBackup<'a> {
    backup: &'a Backup,
    should_keep: bool,
}

fn determine_backups_to_keep(
    opts: CleanupOptions,
    backups: &mut Vec<MarkedBackup>,
    runner: &Runner,
) {
    let now = Local::now();
    // The number of backups we are allowed to keep. We can keep all of them if --force_delete is 0.
    let num_backups = backups.len() as i32;
    let backup_budget = num_backups - opts.force_delete;

    // We always always (always) keep the newest backup.
    backups.last_mut().unwrap().should_keep = true;
    let mut num_saved_backups: i32 = 1;

    // Collect all timestamps
    let mut desired_timestamps = Vec::new();
    for period in &opts.retention_plan.periods {
        desired_timestamps.extend((1..period.instances).map(|i| now - period.interval * i));
    }
    desired_timestamps.sort();
    desired_timestamps.dedup();

    for timestamp in desired_timestamps {
        if num_saved_backups >= num_backups {
            return;
        }
        if num_saved_backups == backup_budget {
            println!("# WARNING --force_delete={} requires us to delete backups that are still within the retention plan.",opts.force_delete);
        }

        for backup in &mut *backups {
            if timestamp < backup.backup.creation_time() {
                runner.verbose(format!(
                    "Keeping {:?} for desired timestamp {}",
                    backup.backup.path(),
                    timestamp.format("%Y-%m-%dT%H:%M:%S")
                ));
                if !backup.should_keep {
                    num_saved_backups += 1;
                }
                backup.should_keep = true;
                break;
            }
        }
    }
}

pub fn run_cleanup_flow_int(repo: Repo, opts: CleanupOptions, runner: &Runner) -> Result<(), String> {
    if repo.num_backups() < 2 {
        runner.commentln("Less than 2 backups were found. We can't cleanup anything.");
        return Ok(());
    }

    let mut backups: Vec<MarkedBackup> = repo
        .backups()
        .iter()
        .map(|backup| MarkedBackup {
            backup,
            should_keep: false,
        })
        .collect();

    determine_backups_to_keep(opts, &mut backups, runner);

    for backup in backups {
        if backup.should_keep {
            runner.verbose(format!("Keep {:?}", backup.backup.path()));
            continue;
        }
        let result = runner.remove_path(backup.backup.path());
        if let Err(v) = result {
            runner.commentln(format!(
                "Error removing backup at path {:?}: {}",
                backup.backup.path(),
                v
            ));
        }
    }
    Ok(())
}
