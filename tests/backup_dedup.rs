mod common;
use biu::run_backup_flow;
use common::*;
use std::collections::HashMap;

#[test]
fn backup_single_file() {
    let f = TestFixture::with_single_source();
    let backup_dir = &f.backup_dir;

    write_files(f.source_dir(), HashMap::from([("a.txt", "Hello World")]));
    write_files(f.source_dir(), HashMap::from([("b/b.foo", "Hello World")]));

    run_backup_flow(biu::BackupFlowOptions {
        initialize: true,
        preserve_mtime: false,
        ..f.backup_flow_options()
    })
    .unwrap();

    // Check that the backup is correct.
    file_trees_equal(
        f.source_dir(),
        &most_recent_backup(&backup_dir).join(f.source_dir().file_name().unwrap()),
    );

    // Check that we didnt'accidentially hardlink the backup to the original.
    let hardlinks = find_all_hardlinks(&backup_dir);
    assert_eq!(hardlinks.len(), 1);
    assert_eq!(
        hardlinks[0],
        vec![f.source_dir().join("a.txt"), f.source_dir().join("b/b.foo")]
    );
}
