use std::{
    collections::HashMap,
    io::Write,
    os::linux::fs::MetadataExt,
    path::{Path, PathBuf},
};

use tempfile::tempdir;

pub struct TestFixture {
    pub test_root: tempfile::TempDir,
    pub source_dirs: Vec<PathBuf>,
    pub backup_dir: PathBuf,
}

impl TestFixture {
    pub fn with_single_source() -> Self {
        let test_root = tempdir().unwrap();
        let source_dir = test_root.path().join("source");
        let backup_dir = test_root.path().join("backup");

        TestFixture {
            test_root,
            source_dirs: vec![source_dir],
            backup_dir,
        }
    }

    pub fn backup_flow_options(&self) -> libbiu::BackupFlowOptions {
        libbiu::BackupFlowOptions {
            initialize: false,
            source_paths: self.source_dirs.clone(),
            backup_path: self.backup_dir.clone(),
            follow_symlinks: false,
            deep_compare: false,
            preserve_mtime: false,
            min_bytes_for_dedup: 0,
        }
    }

    pub fn source_path(&self) -> &Path {
        assert_eq!(self.source_dirs.len(), 1);
        &self.source_dirs[0]
    }
    pub fn source_dir_name(&self) -> &str {
        self.source_path().file_name().unwrap().to_str().unwrap()
    }
}

pub fn write_file(path: &Path, content: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("Cannot open file.");
    file.write_all(content).unwrap();
}

pub fn write_files(path: &Path, files: HashMap<&str, &str>) {
    for (file_name, content) in files {
        write_file(&path.join(file_name), content.as_bytes());
    }
}

pub fn read_dir_recursive(tree: &Path) -> Vec<PathBuf> {
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

pub fn file_trees_equal(lhs: &Path, rhs: &Path) {
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

pub fn find_all_hardlinks(path: &Path) -> Vec<Vec<PathBuf>> {
    let mut inode_to_path: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    for f in read_dir_recursive(path) {
        let metadata = std::fs::metadata(&f).unwrap();
        inode_to_path.entry(metadata.st_ino()).or_default().push(f);
    }

    let mut groups: Vec<Vec<PathBuf>> = inode_to_path
        .values()
        .into_iter()
        .filter(|v| v.len() > 1)
        .cloned()
        .collect();
    groups.sort();
    groups
}

pub fn nth_last_backup(path: &Path, n: usize) -> PathBuf {
    let mut files = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    files.sort();
    files[files.len() - n - 1].to_path_buf()
}

pub fn most_recent_backup(path: &Path) -> PathBuf {
    return nth_last_backup(path, 0);
}
