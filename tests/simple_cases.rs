use biu::run_backup_flow;
use std::{collections::HashMap, fs::File, io::Write, path::Path, path::PathBuf};
use tempfile::tempdir;

fn write_file(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.write_all(content).unwrap();
}

fn write_files(path: &Path, files: HashMap<&str, &str>) {
    for (file_name, content) in files {
        write_file(&path.join(file_name), content.as_bytes());
    }
}

fn read_dir_recursive(tree: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(tree).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            result.extend(read_dir_recursive(&path));
        } else {
            result.push(tree.join(path));
        }
    }
    result.sort();
    result
}

fn file_trees_equal(lhs: &Path, rhs: &Path) {
    let lhs_files = read_dir_recursive(lhs);
    let rhs_files = read_dir_recursive(rhs);

    assert_eq!(lhs_files.len(), rhs_files.len());
    for i in 0..lhs_files.len() {
        let lhs_file = &lhs_files[i];
        let rhs_file = &rhs_files[i];
        assert_eq!(lhs_file.strip_prefix(lhs), rhs_file.strip_prefix(rhs));
        assert!(std::fs::read(lhs_file).unwrap() == std::fs::read(rhs_file).unwrap());
    }
}

fn most_recent_backup(path: &Path) -> PathBuf {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    files.sort();
    files.last().unwrap().to_path_buf()
}

#[test]
fn backup_single_file() {
    let test_root = tempdir().unwrap();
    let source_dir = test_root.path().join("source");
    let backup_dir = test_root.path().join("backup");

    write_files(&source_dir, HashMap::from([("a.txt", "Hello World")]));

    run_backup_flow(biu::BackupFlowOptions {
        initialize: true,
        source_paths: vec![source_dir.to_path_buf()],
        backup_path: backup_dir.to_path_buf(),
        archive_mode: false,
        deep_compare: false,
        preserve_mtime: false,
        min_bytes_for_dedup: 0,
    })
    .unwrap();

    file_trees_equal(
        &source_dir,
        &most_recent_backup(&backup_dir).join(source_dir.file_name().unwrap()),
    );
}
