import Foundation

/// SQLite-backed `VolumeBroker`.
///
/// `DiskBroker` composes the layered storage components and delegates to them;
/// it owns no SQL itself. The layers are:
///   - `SQLiteConnection`  — connection/PRAGMA/schema + serialised read/write access.
///   - `CASVolumeStore`    — content-addressed volume store (store/fetch/has).
///   - `RetainedRootIndex` — named retained-root sets, release, and sweep.
///   - `CountedRootIndex`  — counted roots and their markers.
///
/// Durability: the write connection runs WAL with `synchronous=FULL`, so every
/// committed transaction is fsynced to the WAL before `COMMIT` returns. A
/// `store` that returns has survived any later crash; no separate sync exists.
public final class DiskBroker: @unchecked Sendable, CountedRootBroker {
    public let near: (any VolumeBroker)?
    public let far: (any VolumeBroker)?

    private let connection: SQLiteConnection
    private let volumes: CASVolumeStore
    private let retainedRoots: RetainedRootIndex
    private let countedRoots: CountedRootIndex

    public init(
        path: String,
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) throws {
        let connection = try SQLiteConnection(path: path)
        self.connection = connection
        self.volumes = CASVolumeStore(connection: connection)
        let retainedRoots = RetainedRootIndex(connection: connection)
        self.retainedRoots = retainedRoots
        self.countedRoots = CountedRootIndex(connection: connection, retained: retainedRoots)
        try countedRoots.dropScopesCountingMissingRoots()
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

    @discardableResult
    public func releaseRetainedRoots(scope: String, roots: [String]) async throws -> Int {
        try await retainedRoots.releaseRetainedRoots(scope: scope, roots: roots)
    }

    public func retainedScopes(prefix: String) async throws -> [String] {
        try await retainedRoots.retainedScopes(prefix: prefix)
    }

    // MARK: - Counted Roots

    public func adjustCountedRoots(
        scope: String,
        deltas: [String: Int],
        marker: String,
        expectedMarker: String?
    ) async throws {
        try await countedRoots.adjustCountedRoots(
            scope: scope, deltas: deltas, marker: marker, expectedMarker: expectedMarker)
    }

    public func countedRoots(scope: String) async throws -> [String: Int] {
        try await countedRoots.countedRoots(scope: scope)
    }

    public func countedMarker(scope: String) async throws -> String? {
        try await countedRoots.countedMarker(scope: scope)
    }

    public func countedScopes(prefix: String) async throws -> [String] {
        try await countedRoots.countedScopes(prefix: prefix)
    }

    public func dropCountedScope(scope: String) async throws {
        try await countedRoots.dropCountedScope(scope: scope)
    }

    public func checkpoint() async {
        await connection.checkpoint()
    }

}
