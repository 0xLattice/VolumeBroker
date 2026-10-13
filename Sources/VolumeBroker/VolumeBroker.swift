import Foundation
import cashew

public protocol VolumeBroker: AnyObject, VolumeStorer, ContentSource, Fetcher {
    /// Optional storage tiers in this broker's domain. Cross-chain sources use
    /// separate brokers and are coordinated by the node.
    var near: (any VolumeBroker)? { get }
    var far: (any VolumeBroker)? { get }

    func hasVolume(root: String) async -> Bool
    func fetchVolumeLocal(root: String) async -> SerializedVolume?
    /// Fetch a single node's bytes by its CID when at least one complete local
    /// Volume owns it. The default handles the case where the CID is a root;
    /// CAS-backed brokers also resolve non-root members.
    func fetchDataLocal(cid: String) async -> Data?
    /// Fetch multiple locally owned entries in one backend pass.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data]
    /// Publish the batch all-or-none. A durable broker returns only after the
    /// batch survives a crash.
    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws
}

/// Retention surface: named scopes of retained Volume roots.
///
/// A Volume is kept if a scope retains its root, and only then: retaining a
/// root does not keep a Volume nested under it. `sweep` removes every Volume
/// no scope retains and then every CAS row no surviving Volume owns.
///
/// Storing does not retain. Content stored but not yet named by a retained
/// root is removed by the next `sweep`; a later advance naming it is then
/// refused, so a retained root can never name content the broker lacks.
public protocol RetainedRootBroker: VolumeBroker {
    /// Atomically replace `scope`'s whole root set. Every root must be a
    /// complete stored Volume, or nothing changes. A caller that both merges
    /// and advances one scope must serialize them: an advance built from an
    /// older snapshot drops every root merged after that snapshot.
    func advanceRetainedRoots(scope: String, roots: [String]) async throws
    func retainedRoots(scope: String) async throws -> [String]
    /// Remove every unretained Volume and unowned CAS row in one atomic step.
    /// Returns the number of Volumes removed.
    @discardableResult
    func sweep() async throws -> Int
}

/// Optional retained-root surface for brokers that can atomically add roots to
/// an existing scope without replacing or retransmitting the full scope.
public protocol RetainedRootMergeBroker: RetainedRootBroker {
    func mergeRetainedRoots(scope: String, roots: [String]) async throws
}

/// Optional retained-root surface for brokers that can let go of named roots
/// and reclaim them without a pass over the whole store.
public protocol RetainedRootReleaseBroker: RetainedRootMergeBroker {
    /// Atomically remove `roots` from `scope`, then remove each of those
    /// Volumes that no scope retains and every CAS row that leaves unowned.
    /// A root the scope does not hold is not an error. Nothing but the
    /// Volumes named is touched: content stored and not yet retained stays
    /// unless it is named here. Returns the number of Volumes removed.
    @discardableResult
    func releaseRetainedRoots(scope: String, roots: [String]) async throws -> Int
}

public extension VolumeBroker {
    func store(volume: SerializedVolume) async throws {
        try await storeVolumesLocal([volume])
    }

    func fetch(_ cids: Set<String>) async -> [String: Data] {
        await fetchData(cids: Set(cids.filter { !$0.isEmpty }))
    }

    func fetch(rawCid: String) async throws -> Data {
        guard let data = await fetchData(cid: rawCid) else {
            throw BrokerError.notFound
        }
        return data
    }

    func fetchVolume(root: String) async -> SerializedVolume? {
        if let local = await fetchVolumeLocal(root: root) { return local }
        if let near, let volume = await near.fetchVolume(root: root) { return volume }
        if let far, let volume = await far.fetchVolume(root: root) { return volume }
        return nil
    }

    /// Default content-by-CID lookup for a complete Volume keyed by the CID.
    func fetchDataLocal(cid: String) async -> Data? {
        await fetchVolumeLocal(root: cid)?.entries[cid]
    }

    /// Correct fallback for brokers without a native batch read.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        var found: [String: Data] = [:]
        found.reserveCapacity(cids.count)
        for cid in cids {
            if let data = await fetchDataLocal(cid: cid) { found[cid] = data }
        }
        return found
    }

    /// Content-by-CID across the tier chain (memory -> disk -> network).
    func fetchData(cid: String) async -> Data? {
        if let local = await fetchDataLocal(cid: cid) { return local }
        if let near, let data = await near.fetchData(cid: cid) { return data }
        if let far, let data = await far.fetchData(cid: cid) { return data }
        return nil
    }

    /// Content-by-CID across the tier chain, querying each tier once for only
    /// the entries still missing from preceding tiers.
    func fetchData(cids: Set<String>) async -> [String: Data] {
        var found = await fetchDataLocal(cids: cids)
        var missing = cids.subtracting(found.keys)
        if !missing.isEmpty, let near {
            found.merge(await near.fetchData(cids: missing)) { current, _ in current }
            missing.subtract(found.keys)
        }
        if !missing.isEmpty, let far {
            found.merge(await far.fetchData(cids: missing)) { current, _ in current }
        }
        return found
    }
}
