mod common;
use biu::run_backup_flow;
use common::*;
use std::{collections::HashMap, fs};

#[test]
fn initial_backup_with_dups() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(f.source_path(), HashMap::from([("a.txt", "Hello World")]));
    write_files(f.source_path(), HashMap::from([("b/b.foo", "Hello World")]));

    run_backup_flow(biu::BackupFlowOptions {
        initialize: true,
        preserve_mtime: false,
        ..f.backup_flow_options()
    })
    .unwrap();

    // Check that the backup is correct.
    let newest_backup = most_recent_backup(&backup_dir).join(f.source_dir_name());
    file_trees_equal(f.source_path(), &newest_backup);

    // Check that we didnt'accidentially hardlink the backup to the original.
    let hardlinks = find_all_hardlinks(&backup_dir);
    assert_eq!(hardlinks.len(), 1);
    assert_eq!(
        hardlinks[0],
        vec![newest_backup.join("a.txt"), newest_backup.join("b/b.foo")]
    );
}

#[test]
fn dedup_agaist_old_backup() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    // Backup 1
    write_files(f.source_path(), HashMap::from([("a.txt", "Hello World")]));
    run_backup_flow(biu::BackupFlowOptions {
        initialize: true,
        preserve_mtime: false,
        ..f.backup_flow_options()
    })
    .unwrap();
    // Predate the backup
    let old_time_stamp = (chrono::Local::now() - chrono::Duration::days(1))
        .format("%Y-%m-%d_%H-%M")
        .to_string();
    std::fs::rename(
        most_recent_backup(&backup_dir),
        most_recent_backup(&backup_dir).with_file_name(old_time_stamp),
    )
    .unwrap();

    // Backup 2
    write_files(
        f.source_path(),
        HashMap::from([
            ("a/a.foo", "Hello World"),
            ("b/b.foo", "file b"),
            ("c.orig", "I am file c"),
            ("b/c.foo", "I am file c"),
        ]),
    );
    run_backup_flow(biu::BackupFlowOptions {
        preserve_mtime: false,
        ..f.backup_flow_options()
    })
    .unwrap();

    // Check that the backup is correct.
    let newest_backup = most_recent_backup(&backup_dir).join(f.source_dir_name());
    file_trees_equal(f.source_path(), &newest_backup);

    // Check that we didnt'accidentially hardlink the backup to the original.
    let hardlinks = find_all_hardlinks(&backup_dir);
    assert_eq!(hardlinks.len(), 2);
    assert_eq!(
        hardlinks[0],
        vec![newest_backup.join("a.txt"), newest_backup.join("b/b.foo")]
    );
}
