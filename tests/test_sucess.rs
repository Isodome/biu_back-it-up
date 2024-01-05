use std::{fs::File, io::Write, path::Path};


use biu::run_backup_flow;
use tempfile::tempdir;

fn write_file(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = File::create(path).unwrap();
    file.write_all(b"Hello, world!").unwrap();
}

fn write_test_data(path: &Path) {
    write_file(&path.join("a"), b"Hello, world!");
}

#[test]
fn test_success() {
    let backup_dir = tempdir().unwrap();
    let source_dir = tempdir().unwrap();

    write_test_data(&source_dir.path());
    run_backup_flow(&source_dir.path(), &backup_dir.path()).unwrap();
}
