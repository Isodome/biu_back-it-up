use crate::repo::{LogEntry, Repo, WriteData};
use crate::runner::Runner;
use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{BufReader, Read, self, Bytes};
// use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive()]
pub struct DeduplicationOptions {
    pub deep_compare: bool,
    pub preserve_mtime: bool,
}

pub fn run_deduplication_flow(
    repo: &Repo,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Result<(), String> {
    if repo.has_no_backups() {
        return Ok(());
    }

    let backups = repo.backups();

    // let mut last_file = String::new();
    // let mut last_hash = String::new();
    let mut duplicate_candidates : Vec<WriteData>= Vec::new();

    for log_entry in backups[0].log().iter()? {
        match log_entry? {
            LogEntry::Write(data) => {
                if duplicate_candidates.is_empty() || data.xxh3 == duplicate_candidates[0].xxh3{
                    duplicate_candidates.push(data);
                } else {
                    let absolute_paths = duplicate_candidates.iter().map(|d| backups[0].abs_path(&d.path)).collect::<Vec<PathBuf>>();
                   let status =  maybe_dedup_files(absolute_paths, opts);
                    if let Err(e) = status {
                        runner.commentln(format!(
                            "Failure while trying to eliminate duplicates: {:?}: {}",
                            duplicate_candidates, e
                        ));
                    }
                    duplicate_candidates.clear();
                }
            },
            LogEntry::Unparseable(_) => todo!(),
            _ => {}
        }
    }

    let paths = duplicate_candidates.iter().map(|d| backups[0].abs_path(&d.path)).collect::<Vec<PathBuf>>();
    let status =  maybe_dedup_files(paths, opts);
     if let Err(e) = status {
         runner.commentln(format!(
             "Failure while trying to eliminate duplicates: {:?}: {}",
             duplicate_candidates, e
         ));
     }

    Ok(())
}

fn replace_file_with_link(original: &Path, duplicate: &Path) {
    println!("{:?} and {:?} are dups.", original, duplicate);

  let basedir = duplicate.parent().expect("We could not determine the basedir of a file.");
  let file_name = duplicate.file_name().expect("Unable to deterimne the filename of a file.");

  let mut tmp_file_name = basedir.join(format!("{}.as_link", file_name.to_string_lossy()));
  let mut i = 0;
  while tmp_file_name.exists() {
     tmp_file_name = basedir.join(format!("{}.as_link{i}", file_name.to_string_lossy()));
     i += 1;
  }
  std::fs::hard_link(&original, &tmp_file_name).expect(format!("Failed to create hard link to replace {}", duplicate.to_string_lossy()).as_str());
  if let Err(_e) =   std::fs::rename(&tmp_file_name, &duplicate) {
    // If the renaming fails we delete the hardlink created above.
    let _ = std::fs::remove_file(tmp_file_name);
  }
}
fn as_path_refs(pathbufs : &Vec<PathBuf> ) -> Vec<&Path> {
    return pathbufs.iter().map(|pathbuf| pathbuf.as_path()).collect();
}
fn maybe_dedup_files (  duplicates:  Vec<PathBuf>, opts : &DeduplicationOptions) -> io::Result<()> {
    if duplicates.len() < 2 {
        return Ok(())
    }

    let mut duplicate_candidates = as_path_refs(&duplicates);
    while duplicate_candidates.len( ) > 1 {
        let true_dups = find_all_real_dups(&duplicate_candidates[0], &duplicate_candidates[1..], opts)?;
        for true_dup in &true_dups {
            replace_file_with_link(&duplicate_candidates[0], true_dup)
        }
        if true_dups.len() == duplicate_candidates.len()-1 {
            // In case all dups by hash are true dups (so almost always) we can take this shortcut.
            return Ok(());
        }
        duplicate_candidates.remove(0);
        duplicate_candidates.retain(|path| !true_dups.contains(&path.as_ref()));
    }
    return Ok(());
}


fn find_all_real_dups<'b>(
    original: &Path,
    duplicates: & [&'b Path],
    opts: &DeduplicationOptions,
) -> std::io::Result<Vec<&'b Path>> {

    let  mut true_dups = Vec::from(duplicates);
    // let stat_original = std::fs::symlink_metadata(original)?;
    // let stat_duplicate = std::fs::symlink_metadata(duplicate)?;

    // if stat_original.size() != stat_duplicate.size() {
    //     return Ok(false);
    // }
    // if opts.preserve_mtime && stat_original.mtime() != stat_duplicate.mtime() {
    //     return Ok(false);
    // }
    if opts.deep_compare  {
        return find_all_dups_by_content(original, &true_dups);
    }
    return Ok(true_dups);
}


/// 
fn find_all_dups_by_content<'a>(original: &Path, files: &  [&'a Path]) -> std::io::Result<Vec<&'a Path>> {
    struct FileAndPath<'a> {
        bytes: Bytes<BufReader<File>>,
        path: &'a Path,
    }
    let buffer_orig = BufReader::new(File::open(original)?);
    let mut files  = files.iter()
    .map(|path|-> io::Result<FileAndPath> 
        {let file =  File::open(path)?;
        let reader =  BufReader::new(file);
        return Ok(FileAndPath{bytes:reader.bytes(), path:path});
        })
    .collect::<io::Result<Vec<FileAndPath>>>()?;


    for original_byte in buffer_orig.bytes() {
        let b = original_byte?;
            files.retain_mut(|file|{
                file.bytes.next().expect("Trying to compare files with different lenghts.").expect("Error reading file") == b
            });
    }

    return Ok(files.iter().map(|file|file.path).collect());
}

struct FileBatch {
    min_hash: String,
    max_hash: String,
    hash_to_path: HashMap<String, String>,
}
