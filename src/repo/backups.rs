use std::fs;
use std::io;
use std::path::Path;



fn get_directories(path: &str) -> Result<Vec<String>, io::Error> {
    let directories: Result<Vec<String>, io::Error> = fs::read_dir(path)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let metadata = entry.metadata().ok()?;
            if metadata.is_dir() {
                entry.file_name().to_str().map(String::from)
            } else {
                None
            }
        })
        .collect();

    directories
}

pub fn list_backups(path : Path) -> Result<Vec<Backup>, Error> {
    if !path.is_dir() {
        return Error
    }

}