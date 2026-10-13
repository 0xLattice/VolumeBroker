import Foundation

/// SQLite-backed `VolumeBroker`.
///
/// `DiskBroker` composes the layered storage components and delegates to them;
/// it owns no SQL itself. The layers are:
///   - `SQLiteConnection`  — connection/PRAGMA/schema + serialised read/write access.
///   - `CASVolumeStore`    — content-addressed volume store (store/fetch/has).
///   - `RetainedRootIndex` — named retained-root sets and sweep.
///
/// Durability: the write connection runs WAL with `synchronous=FULL`, so every
/// committed transaction is fsynced to the WAL before `COMMIT` returns. A
/// `store` that returns has survived any later crash; no separate sync exists.
public final class DiskBroker: @unchecked Sendable, RetainedRootMergeBroker {
    public let near: (any VolumeBroker)?
    public let far: (any VolumeBroker)?

    private let connection: SQLiteConnection
    private let volumes: CASVolumeStore
    private let retainedRoots: RetainedRootIndex

    public init(
        path: String,
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) throws {
        let connection = try SQLiteConnection(path: path)
        self.connection = connection
        self.volumes = CASVolumeStore(connection: connection)
        self.retainedRoots = RetainedRootIndex(connection: connection)
        self.near = near
        self.far = far
    }

    // MARK: - VolumeBroker

    public func hasVolume(root: String) async -> Bool {
        await volumes.hasVolume(root: root)
    }

    public func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await volumes.fetchVolumeLocal(root: root)
    }

    public func fetchDataLocal(cid: String) async -> Data? {
        await volumes.fetchDataLocal(cid: cid)
    }

    public func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        await volumes.fetchDataLocal(cids: cids)
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        try await self.volumes.storeVolumesLocal(volumes)
    }

    // MARK: - Retained Roots

    public func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.advanceRetainedRoots(scope: scope, roots: roots)
    }

    public func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.mergeRetainedRoots(scope: scope, roots: roots)
    }

    public func retainedRoots(scope: String) async throws -> [String] {
        try await retainedRoots.retainedRoots(scope: scope)
    }

    @discardableResult
    public func sweep() async throws -> Int {
        try await retainedRoots.sweep()
    }

    public func checkpoint() async {
        await connection.checkpoint()
    }

}
