import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Counts the syncs SQLite asks of each file, through a VFS that wraps the
/// default one: what a commit or a flush really synced, seen where SQLite
/// asks for it. Once installed it is the process's default VFS, and only
/// counts.
enum SyncCountingVFS {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var installed = false
    nonisolated(unsafe) private static var base: UnsafeMutablePointer<sqlite3_vfs>?
    /// Open file -> its path.
    nonisolated(unsafe) private static var paths: [UnsafeMutableRawPointer: String] = [:]
    nonisolated(unsafe) private static var counts: [String: Int] = [:]
    /// The default VFS's method tables and their counting copies, each way.
    nonisolated(unsafe) private static var counting: [UnsafeRawPointer: UnsafeMutablePointer<sqlite3_io_methods>] = [:]
    nonisolated(unsafe) private static var originals: [UnsafeRawPointer: UnsafePointer<sqlite3_io_methods>] = [:]

    static func install() {
        lock.lock()
        defer { lock.unlock() }
        guard !installed, let found = sqlite3_vfs_find(nil) else { return }
        installed = true
        base = found
        let wrapper = UnsafeMutablePointer<sqlite3_vfs>.allocate(capacity: 1)
        wrapper.initialize(to: found.pointee)
        wrapper.pointee.zName = UnsafePointer(strdup("sync-counting"))
        wrapper.pointee.pNext = nil
        wrapper.pointee.xOpen = { _, name, file, flags, outFlags in
            SyncCountingVFS.lock.lock()
            let wrapped = SyncCountingVFS.base
            SyncCountingVFS.lock.unlock()
            guard let base = wrapped, let open = base.pointee.xOpen else { return SQLITE_ERROR }
            let status = open(base, name, file, flags, outFlags)
            guard status == SQLITE_OK, let file, let methods = file.pointee.pMethods else { return status }
            SyncCountingVFS.lock.lock()
            defer { SyncCountingVFS.lock.unlock() }
            let key = UnsafeRawPointer(methods)
            let copy: UnsafeMutablePointer<sqlite3_io_methods>
            if let made = SyncCountingVFS.counting[key] {
                copy = made
            } else {
                copy = .allocate(capacity: 1)
                copy.initialize(to: methods.pointee)
                copy.pointee.xSync = { file, flags in
                    guard let file, let methods = file.pointee.pMethods else { return SQLITE_ERROR }
                    SyncCountingVFS.lock.lock()
                    let original = SyncCountingVFS.originals[UnsafeRawPointer(methods)]
                    if let path = SyncCountingVFS.paths[UnsafeMutableRawPointer(file)] {
                        SyncCountingVFS.counts[path, default: 0] += 1
                    }
                    SyncCountingVFS.lock.unlock()
                    guard let sync = original?.pointee.xSync else { return SQLITE_ERROR }
                    return sync(file, flags)
                }
                SyncCountingVFS.counting[key] = copy
                SyncCountingVFS.originals[UnsafeRawPointer(copy)] = methods
            }
            file.pointee.pMethods = UnsafePointer(copy)
            SyncCountingVFS.paths[UnsafeMutableRawPointer(file)] = name.map { String(cString: $0) }
            return status
        }
        sqlite3_vfs_register(wrapper, 1)
    }

    /// Syncs so far of the file whose path ends with `suffix`.
    static func syncs(ofFileEndingWith suffix: String) -> Int {
        lock.lock()
        defer { lock.unlock() }
        return counts.reduce(0) { $1.key.hasSuffix(suffix) ? $0 + $1.value : $0 }
    }
}
