import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// A disk that can lose power, for tests of what an outage leaves.
///
/// A power cut wipes what the operating system held in memory: every write
/// since a file's last sync. This is a SQLite VFS, asked for by name, that
/// passes every call to the default one and remembers each file as it was
/// when it was last synced. `afterOutage` writes those remembered files to
/// another directory: the store an operator finds when the power is back.
///
/// It loses every unsynced write, whole. A real outage may keep some of
/// them; losing all of them is one thing that can happen, and the hardest
/// for a claim that something written is still there.
enum PowerLossDisk {
    static let vfsName = "power-loss"

    private static let lock = NSLock()
    nonisolated(unsafe) private static var installed = false
    nonisolated(unsafe) private static var base: UnsafeMutablePointer<sqlite3_vfs>?
    /// Open file -> its path.
    nonisolated(unsafe) private static var paths: [UnsafeMutableRawPointer: String] = [:]
    /// Path -> the file's bytes at its last sync.
    nonisolated(unsafe) private static var synced: [String: Data] = [:]
    /// The default VFS's method tables and their recording copies, each way.
    nonisolated(unsafe) private static var recording: [UnsafeRawPointer: UnsafeMutablePointer<sqlite3_io_methods>] = [:]
    nonisolated(unsafe) private static var originals: [UnsafeRawPointer: UnsafePointer<sqlite3_io_methods>] = [:]

    /// Registers the VFS, once. It is not made the default: only a store
    /// opened by its name runs on it.
    static func install() {
        lock.lock()
        defer { lock.unlock() }
        guard !installed, let found = sqlite3_vfs_find(nil) else { return }
        installed = true
        base = found
        let wrapper = UnsafeMutablePointer<sqlite3_vfs>.allocate(capacity: 1)
        wrapper.initialize(to: found.pointee)
        wrapper.pointee.zName = UnsafePointer(strdup(vfsName))
        wrapper.pointee.pNext = nil
        wrapper.pointee.xOpen = { _, name, file, flags, outFlags in
            PowerLossDisk.lock.lock()
            let wrapped = PowerLossDisk.base
            PowerLossDisk.lock.unlock()
            guard let base = wrapped, let open = base.pointee.xOpen else { return SQLITE_ERROR }
            let status = open(base, name, file, flags, outFlags)
            guard status == SQLITE_OK, let file, let methods = file.pointee.pMethods else { return status }
            PowerLossDisk.lock.lock()
            defer { PowerLossDisk.lock.unlock() }
            let key = UnsafeRawPointer(methods)
            let copy: UnsafeMutablePointer<sqlite3_io_methods>
            if let made = PowerLossDisk.recording[key] {
                copy = made
            } else {
                copy = .allocate(capacity: 1)
                copy.initialize(to: methods.pointee)
                copy.pointee.xSync = { file, flags in
                    guard let file, let methods = file.pointee.pMethods else { return SQLITE_ERROR }
                    PowerLossDisk.lock.lock()
                    let original = PowerLossDisk.originals[UnsafeRawPointer(methods)]
                    let path = PowerLossDisk.paths[UnsafeMutableRawPointer(file)]
                    PowerLossDisk.lock.unlock()
                    guard let sync = original?.pointee.xSync else { return SQLITE_ERROR }
                    let status = sync(file, flags)
                    // Synced: this is the file a power cut now leaves.
                    if status == SQLITE_OK, let path, let bytes = FileManager.default.contents(atPath: path) {
                        PowerLossDisk.lock.lock()
                        PowerLossDisk.synced[path] = bytes
                        PowerLossDisk.lock.unlock()
                    }
                    return status
                }
                PowerLossDisk.recording[key] = copy
                PowerLossDisk.originals[UnsafeRawPointer(copy)] = methods
            }
            file.pointee.pMethods = UnsafePointer(copy)
            PowerLossDisk.paths[UnsafeMutableRawPointer(file)] = name.map { String(cString: $0) }
            return status
        }
        sqlite3_vfs_register(wrapper, 0)
    }

    /// The store at `path` as a power cut now would leave it, written into
    /// `directory`: the database and its WAL as each was last synced, a
    /// file never synced absent. Returns the database's path there.
    static func afterOutage(of path: String, into directory: URL) throws -> String {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let name = URL(fileURLWithPath: path).lastPathComponent
        // SQLite names a file by its real path (/private/var for /var).
        guard let real = realpath(path, nil) else { throw CocoaError(.fileNoSuchFile) }
        defer { free(real) }
        let resolved = String(cString: real)
        lock.lock()
        let files = ["", "-wal"].compactMap { suffix -> (String, Data)? in
            synced[resolved + suffix].map { (name + suffix, $0) }
        }
        lock.unlock()
        for (file, bytes) in files {
            try bytes.write(to: directory.appendingPathComponent(file))
        }
        return directory.appendingPathComponent(name).path
    }
}
