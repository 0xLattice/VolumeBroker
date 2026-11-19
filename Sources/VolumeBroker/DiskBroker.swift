import Foundation

/// When a `DiskBroker` write is durable.
///
/// Either way a write is atomic and writes are ordered: a store opened after
/// any crash holds a prefix of the committed writes, each whole.
public enum BrokerDurability: Sendable, Equatable {
    /// A write is durable when it returns: every commit waits for its fsync.
    case everyCommit
    /// A write does not wait for its fsync. A crash of the process loses
    /// nothing; a power loss or a kernel crash loses the writes since the
    /// last `flush`. For a store whose content can be had again, where that
    /// wait is the cost and the loss is a re-fetch.
    ///
    /// The caller keeps two orders. Anything another store makes durable
    /// that names this broker's content comes after a `flush`, so it never
    /// names content a power loss took. And a write the operator relies on
    /// (a retained or counted root it was promised) is acknowledged after a
    /// `flush`.
    case deferred
}

/// SQLite-backed `VolumeBroker`.
///
/// `DiskBroker` composes the layered storage components and delegates to them;
/// it owns no SQL itself. The layers are:
///   - `SQLiteConnection`  — connection/PRAGMA/schema + serial writes, pooled reads.
///   - `CASVolumeStore`    — content-addressed volume store (store/fetch/has).
///   - `RetainedRootIndex` — named retained-root sets, release, and sweep.
///   - `CountedRootIndex`  — counted roots and their markers.
///
/// Durability is the opener's choice (`BrokerDurability`). By default the write
/// connection runs WAL with `synchronous=FULL`, so every committed transaction
/// is fsynced to the WAL before `COMMIT` returns: a `store` that returns has
/// survived any later crash. With `.deferred` a commit does not wait for its
/// sync, and `flush` makes every commit so far durable.
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
        far: (any VolumeBroker)? = nil,
        durability: BrokerDurability = .everyCommit
    ) throws {
        let connection = try SQLiteConnection(path: path, durability: durability)
        self.connection = connection
        self.volumes = CASVolumeStore(connection: connection)
        let retainedRoots = RetainedRootIndex(connection: connection)
        self.retainedRoots = retainedRoots
        self.countedRoots = CountedRootIndex(connection: connection, retained: retainedRoots)
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

    public func storeVolumesLocal(_ volumes: [ValidatedVolume]) async throws {
        try await self.volumes.storeVolumesLocal(volumes, then: nil)
    }

    // MARK: - Retained Roots

    public func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.advanceRetainedRoots(scope: scope, roots: roots)
    }

    public func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        try await retainedRoots.mergeRetainedRoots(scope: scope, roots: roots)
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume], retaining: [String: [String]]) async throws {
        let merges = try RetainedRootIndex.canonicalMerges(retaining)
        try await store(volumes.map(ValidatedVolume.init(validating:)), merges: merges)
    }

    public func storeVolumesLocal(_ volumes: [ValidatedVolume], retaining: [String: [String]]) async throws {
        try await store(volumes, merges: try RetainedRootIndex.canonicalMerges(retaining))
    }

    private func store(_ volumes: [ValidatedVolume], merges: [(scope: String, roots: [String])]) async throws {
        let index = self.retainedRoots
        @Sendable func retain() throws { try index.merge(merges) }
        try await self.volumes.storeVolumesLocal(volumes, then: merges.isEmpty ? nil : retain)
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

    @discardableResult
    public func releaseRetainedScopes(_ scopes: [String]) async throws -> Int {
        try await retainedRoots.releaseRetainedScopes(scopes)
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

    public func countedState(scope: String) async throws -> (marker: String?, counts: [String: Int]) {
        try await countedRoots.countedState(scope: scope)
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

    /// Makes every write that has returned durable. Nothing to do, and
    /// nothing done, unless the broker was opened `.deferred`. A flush that
    /// throws leaves those writes' durability unknown; a later flush that
    /// returns does not prove them durable, since a failed fsync may have
    /// dropped them. Treat a throw as a storage fault.
    public func flush() async throws {
        try await connection.flush()
    }

}
