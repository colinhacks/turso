use super::*;

#[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
use crate::WindowsIOCP;

#[test]
fn open_db_async_state_drop_clears_opening_registry_entry() {
    let key = DatabaseKey::SharedMemory(
        "open-db-async-state-drop-clears-opening-registry-entry".to_string(),
    );
    {
        let mut manager = DATABASE_MANAGER.lock();
        manager.insert(key.clone(), RegistryEntry::Opening);
    }

    let mut state = OpenDbAsyncState::new();
    state.registry_key = Some(key.clone());
    drop(state);

    let manager = DATABASE_MANAGER.lock();
    assert!(
        !manager.contains_key(&key),
        "dropping an incomplete async open must clear the Opening sentinel"
    );
}

#[test]
#[cfg(any(target_os = "linux", target_os = "android"))]
fn shared_wal_coordination_rejects_remote_filesystem_magic_values() {
    assert!(!Database::filesystem_magic_allows_shared_wal(0x6969));
    assert!(!Database::filesystem_magic_allows_shared_wal(
        0xFF53_4D42u32 as libc::c_long,
    ));
    assert!(!Database::filesystem_magic_allows_shared_wal(0x0102_1997));
    assert!(Database::filesystem_magic_allows_shared_wal(0xEF53));
    assert!(Database::filesystem_magic_allows_shared_wal(0x0102_1994));
}

#[test]
#[cfg(host_shared_wal)]
fn shared_wal_coordination_path_probe_accepts_nonexistent_relative_paths() {
    let result = Database::path_allows_shared_wal_coordination(std::path::Path::new(
        "nonexistent-relative-multiprocess-wal.db",
    ));
    assert!(
        result.is_ok(),
        "nonexistent relative paths should probe the current directory instead of erroring: {result:?}"
    );
}
