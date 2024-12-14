mod common;

use std::collections::HashMap;
use std::fs;

use common::*;
use biu::{run_cleanup_flow, CleanupFlowOptions, RetentionPlan};

#[test]
fn no_repo() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    let retention_plan = RetentionPlan::default();

    run_cleanup_flow(CleanupFlowOptions {
        backup_path: &backup_dir,
        retention_plan: &retention_plan,
        force_delete: 0,
    })
    .expect_err("Expected error when no repo is present.");
}

#[test]
fn empty_repo() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;
    fs::create_dir_all(backup_dir).unwrap();

    let retention_plan = RetentionPlan::default();

    run_cleanup_flow(CleanupFlowOptions {
        backup_path: &backup_dir,
        retention_plan: &retention_plan,
        force_delete: 0,
    })
    .unwrap();
}

#[test]
fn empty_delete_old_backups() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;
    let latest_backup = backup_dir.join("2024-04-11_21-52");

    write_files(
        f.source_path(),
        HashMap::from([
            ("2024-04-11_21-52/a.txt", "Hello World"),
            ("2024-04-12_20-52/a.txt", "Hello World"),
            ("2024-04-12_22-52/a.txt", "Hello World"),
            ("2024-05-11_21-52/a.txt", "Hello World"),
        ]),
    );

    fs::create_dir_all(latest_backup).unwrap();

    let retention_plan: RetentionPlan = "7*1d".parse().unwrap();

    run_cleanup_flow(CleanupFlowOptions {
        backup_path: &backup_dir,
        retention_plan: &retention_plan,
        force_delete: 10,
    })
    .unwrap();

    assert!(backup_dir.exists());
}

#[test]
fn empty_one_backup_force_delete() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;
    let latest_backup = backup_dir.join("2024-04-11_21-52");
    fs::create_dir_all(&latest_backup).unwrap();

    let retention_plan = RetentionPlan::default();

    run_cleanup_flow(CleanupFlowOptions {
        backup_path: &backup_dir,
        retention_plan: &retention_plan,
        force_delete: 10,   
    })
    .unwrap();

    assert!(latest_backup.exists());
}
