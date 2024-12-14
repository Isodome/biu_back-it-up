mod common;
use common::*;
use std::{collections::HashMap, path::Path};

use assert_cmd::Command;

#[test]
fn test_biu_cli_deduplication() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(
        f.source_path(),
        HashMap::from([
            ("file1.txt", "Hello World"),
            ("file2.txt", "This is unique test data"),
            ("a/file3", "Hello World"),
        ]),
    );

    // Do an initial backup
    {
        let mut cmd = Command::cargo_bin("biu").expect("`biu` binary not found");
        cmd.env(
            "LD_PRELOAD",
            "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
        )
        .env("FAKETIME", "2024-01-01 10:00:00")
        .arg("backup")
        .arg("--source-paths")
        .arg(f.source_path())
        .arg("--backup-path")
        .arg(backup_dir)
        .arg("--initialize");
        cmd.assert().success();
    }

    file_trees_equal(
        f.source_path(),
        &backup_dir
            .join(Path::new("2024-01-01_10-00"))
            .join(f.source_path().file_name().unwrap()),
    );

    {
        let mut cmd = Command::cargo_bin("biu").expect("`biu` binary not found");
        cmd.env(
            "LD_PRELOAD",
            "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
        )
        .env("FAKETIME", "2024-01-01 10:00:00")
        .arg("backup")
        .arg("--source-paths")
        .arg(f.source_path())
        .arg("--backup-path")
        .arg(backup_dir);
        cmd.assert().success();

        file_trees_equal(f.source_path(), back_dir_with_name("2024-01-01_10-00", &f));
        file_trees_equal(
            back_dir_with_name("2024-01-01_10-00", &f),
            back_dir_with_name("2024-01-01_10-00_1", &f),
        );

        print!("{:?}", find_all_hardlinks(&f.backup_dir));
    }
}
