
// derive[Debug]
pub struct BackupOptions {
    snapshot_date_pattern: String,
    source_paths: &[Path],
    backup_path: Path,
}


pub fn backup_command(opts : &BackupOptions) ->Result<(), String> {
    
}