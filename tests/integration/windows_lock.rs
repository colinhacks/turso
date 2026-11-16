#[allow(unused_imports)]
use std::os::windows::ffi::OsStrExt;
#[allow(unused_imports)]
use std::os::windows::ffi::OsStringExt;
use std::path::Path;
use std::path::PathBuf;
use turso_core::io::windows_lock::*;
use turso_core::sync::Mutex;

use std::os::windows::io::AsRawHandle;

struct CurrentDirGuard(PathBuf);

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.0).expect("restore current directory");
    }
}

#[test]
fn shared_wal_lock_uses_cached_path_after_current_dir_change() {
    let current_dir = std::env::current_dir().expect("read current directory");
    let _restore = CurrentDirGuard(current_dir);
    let root = tempfile::tempdir().expect("create root directory");
    let first_dir = root.path().join("first");
    let second_dir = root.path().join("second");
    std::fs::create_dir_all(&first_dir).expect("create first directory");
    std::fs::create_dir_all(&second_dir).expect("create second directory");
    std::fs::write(first_dir.join("coordination.tshm"), []).expect("create coordination file");

    std::env::set_current_dir(&first_dir).expect("enter first directory");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("coordination.tshm")
        .expect("open coordination file");
    let path = stable_lock_path_for_handle(file.as_raw_handle(), Path::new("coordination.tshm"));
    let state = Mutex::new(SharedWalLockState::default());
    shared_wal_lock_byte(&path, &state, 0, false).expect("acquire shared lifetime lock");

    std::env::set_current_dir(&second_dir).expect("enter second directory");
    shared_wal_unlock_byte(&path, &state, 0).expect("release shared lifetime lock");
    release_shared_wal_locks_on_drop(&path, &state);
}
