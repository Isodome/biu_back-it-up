use serde::{Deserialize, Serialize};

use crate::utils::Interval;

#[derive(Deserialize, Serialize)]
pub struct BackupStats {
    // Stats
    pub num_writes: i32,
    pub num_hardlinks: i32,
    pub num_deletes: i32,
    pub bytes_written: u64,
    pub bytes_deleted: u64,
    pub min_mtime: Option<i64>,
    pub max_mtime: Option<i64>,
    pub min_mtime_written: Option<i64>,
    pub max_mtime_written: Option<i64>,
    pub backup_begin_mtime: u64,
    pub backup_end_mtime: Option<u64>,
}

impl BackupStats {
    pub fn new() -> BackupStats {
        let since_the_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to get system time.")
            .as_secs();
        BackupStats {
            num_writes: 0,
            num_hardlinks: 0,
            num_deletes: 0,
            bytes_written: 0,
            bytes_deleted: 0,
            min_mtime: None,
            max_mtime: None,
            min_mtime_written: None,
            max_mtime_written: None,
            backup_begin_mtime: since_the_epoch,
            backup_end_mtime: None,
        }
    }

    pub fn update_mtime(&mut self, mtime: i64) {
        let as_opt = Some(mtime);
        self.min_mtime = std::cmp::min(self.min_mtime.or(as_opt), as_opt);
        self.max_mtime = std::cmp::max(self.max_mtime.or(as_opt), as_opt);
    }

    pub fn update_mtime_written(&mut self, mtime: i64) {
        let as_opt = Some(mtime);
        self.min_mtime_written = std::cmp::min(self.min_mtime_written.or(as_opt), as_opt);
        self.max_mtime_written = std::cmp::max(self.max_mtime_written.or(as_opt), as_opt);
    }

    pub fn report_write(&mut self, size: u64, mtime: i64) {
        self.num_writes += 1;
        self.bytes_written += size;
        self.update_mtime(mtime);
        self.update_mtime_written(mtime);
    }

    pub fn report_link(&mut self, mtime: i64) {
        self.num_hardlinks += 1;
        self.update_mtime(mtime);
    }
    pub fn report_delete(&mut self, size: u64) {
        self.num_deletes += 1;
        self.bytes_deleted += size;
    }
    pub fn report_done(&mut self) {
        self.backup_end_mtime = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Failed to get system time.")
                .as_secs(),
        );
    }

    pub fn mtimes(&self) -> Interval<i64> {
        return Interval {
            lo: self.min_mtime.unwrap_or(1),
            hi: self.max_mtime.unwrap_or(0),
        };
    }
    pub fn mtimes_written(&self) -> Interval<i64> {
        return Interval {
            lo: self.min_mtime_written.unwrap_or(1),
            hi: self.max_mtime_written.unwrap_or(0),
        };
    }

    pub fn as_toml(&self) -> String {
        return toml::to_string_pretty(self).expect("Serialization failed.");
    }

    pub fn from_toml(s: &str) -> BackupStats {
        let stats: BackupStats = toml::from_str(s).expect("Invalid TOML string.");
        return stats;
    }
}
