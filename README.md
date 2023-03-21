# biu - Back It Up!
A complete backup solution built around rsync.

The idea is to have a backup-tool do regular backups (e.g. daily) and use this program to 


### Usage

```
biu <command> [<options>]

commands:
    * backup                        Create a backup
    * cleanup                       Delete old backups according to the rentention plan
    * dedup                         Replace duplicate files with hardlinks
    * scrub                         Verify checksums
options
    -n, --dry_run:                   Outputs the backup's steps as script to stdout instead of executing them.
    -s, --source <path>:             The directory to backup. -s can be repeated.
    -b, --backup_path <path>         A writeable directory for the backups.
    -p, --retention_plan <plan>:     A string describing which backups to retain during cleanup (details below)
    -a, --archive:                   Use rsync in archive mode (-a). This will maintain owners, groups and permissions in the backup.
    -f, --force_delete <n>           If >0, `cleanup` will delete at least n backups. (Exception: The newest backup will never be deleted).




Retention Plans:

The retention plan serves two functions. It specifies which backups should be kept in normal operation. It also specifies the order in which backups will be deleted if force_delete is used but all backups are still within the retention plan. The format of a retention plan is <duration>:<number of backups>,... e.g. *1d:14,1w:8* means: Keep daily (1d) backups for 14 days and weekly backups for 8 weeks.

The cleanup command will delete all backups that aren't covered by the retention plan. If all backups are covered by the retention plan and --force_delete > 0 we'll priotize the items of the retention plan that are mentioned first. 

The default retention plan is *1d:2,1w:4,1d:14,1w:8,1m:60*.

```