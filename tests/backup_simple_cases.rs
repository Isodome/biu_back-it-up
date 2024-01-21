mod common;
use libbiu::run_backup_flow;
use common::*;
use std::collections::HashMap;

#[test]
fn backup_single_file() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(f.source_path(), HashMap::from([("a.txt", "Hello World")]));

    run_backup_flow(libbiu::BackupFlowOptions {
        initialize: true,
        ..f.backup_flow_options()
    })
    .unwrap();

    // Check that the backup is correct.
    file_trees_equal(
        f.source_path(),
        &most_recent_backup(&backup_dir).join(f.source_path().file_name().unwrap()),
    );

    // Check that we didnt'accidentially hardlink the backup to the original.
    assert!(find_all_hardlinks(&backup_dir).is_empty());
}

#[test]
fn repository_not_initialized() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(f.source_path(), HashMap::from([("a.txt", "Hello World")]));

    let status = run_backup_flow(libbiu::BackupFlowOptions {
        initialize: false,
        ..f.backup_flow_options()
    });

    assert!(status.is_err());

    assert!(!backup_dir.exists());
}

#[test]
fn initialize_fails_if_repository_exists() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(f.source_path(), HashMap::from([("a.txt", "Hello World")]));
    std::fs::create_dir_all(&backup_dir).unwrap();

    let status = run_backup_flow(libbiu::BackupFlowOptions {
        initialize: true,
        ..f.backup_flow_options()
    });

    assert!(status.is_err());
}
