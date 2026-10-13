import Foundation

/// In-memory twin of `DiskBroker` with identical store, retention, and sweep
/// semantics.
public final class MemoryBroker: @unchecked Sendable, RetainedRootReleaseBroker {
    public let near: (any VolumeBroker)?
    public let far: (any VolumeBroker)?

    private struct State {
        var contentByCID: [String: Data] = [:]
        var membersByRoot: [String: Set<String>] = [:]
        var ownersByCID: [String: Set<String>] = [:]
        /// Keyed by a scope name's bytes, as SQLite compares them: two
        /// spellings Swift calls equal are two scopes.
        var retainedRoots: [[UInt8]: Set<String>] = [:]
    }

    private let lock = RWLock()
    private var state = State()

    public init(
        near: (any VolumeBroker)? = nil,
        far: (any VolumeBroker)? = nil
    ) {
        self.near = near
        self.far = far
    }

    public func hasVolume(root: String) async -> Bool {
        lock.withReadLock { Self.isComplete(root: root, state: state) }
    }

    public func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        lock.withReadLock { Self.volume(root: root, state: state) }
    }

    public func fetchDataLocal(cid: String) async -> Data? {
        await fetchDataLocal(cids: [cid])[cid]
    }

    public func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        lock.withReadLock {
            var found: [String: Data] = [:]
            found.reserveCapacity(cids.count)
            for cid in cids {
                guard let data = state.contentByCID[cid],
                      let owners = state.ownersByCID[cid],
                      !owners.isEmpty else { continue }
                found[cid] = data
            }
            return found
        }
    }

    public func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        let volumes = volumes.map { $0.ownedCopy() }
        for volume in volumes { try volume.validate() }
        guard !volumes.isEmpty else { return }
        try lock.withWriteLock {
            let newVolumes = try Self.preflight(volumes, state: state)
            for volume in newVolumes {
                state.membersByRoot[volume.root] = Set(volume.entries.keys)
                for (cid, data) in volume.entries {
                    state.contentByCID[cid] = data
                    state.ownersByCID[cid, default: []].insert(volume.root)
                }
            }
        }
    }

    private static func preflight(_ volumes: [SerializedVolume], state: State) throws -> [SerializedVolume] {
        var pendingMemberships: [String: Set<String>] = [:]
        var pendingContent: [String: Data] = [:]
        var newVolumes: [SerializedVolume] = []

        for volume in volumes {
            let members = Set(volume.entries.keys)
            if let existing = pendingMemberships[volume.root] ?? state.membersByRoot[volume.root],
               existing != members {
                throw BrokerError.conflictingVolume(volume.root)
            }
            if pendingMemberships[volume.root] == nil {
                pendingMemberships[volume.root] = members
                if state.membersByRoot[volume.root] == nil {
                    newVolumes.append(volume)
                }
            }

            for (cid, data) in volume.entries {
                if let existing = pendingContent[cid] ?? state.contentByCID[cid],
                   existing != data {
                    throw BrokerError.conflictingContent(cid)
                }
                pendingContent[cid] = data
            }
        }
        return newVolumes
    }

    private static func isComplete(root: String, state: State) -> Bool {
        guard let members = state.membersByRoot[root],
              !members.isEmpty,
              members.contains(root) else { return false }
        return members.allSatisfy { cid in
            state.contentByCID[cid] != nil && state.ownersByCID[cid]?.contains(root) == true
        }
    }

    private static func volume(root: String, state: State) -> SerializedVolume? {
        guard isComplete(root: root, state: state),
              let members = state.membersByRoot[root] else { return nil }
        var entries: [String: Data] = [:]
        entries.reserveCapacity(members.count)
        for cid in members {
            guard let data = state.contentByCID[cid] else { return nil }
            entries[cid] = data
        }
        return SerializedVolume(root: root, entries: entries)
    }

    private static func removeVolume(root: String, state: inout State) {
        guard let members = state.membersByRoot.removeValue(forKey: root) else { return }
        for cid in members {
            var owners = state.ownersByCID[cid] ?? []
            owners.remove(root)
            if owners.isEmpty {
                state.ownersByCID.removeValue(forKey: cid)
                state.contentByCID.removeValue(forKey: cid)
            } else {
                state.ownersByCID[cid] = owners
            }
        }
    }

    // MARK: - Retained Roots

    public func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        try Self.validateScope(scope)

        try lock.withWriteLock {
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            state.retainedRoots[Array(scope.utf8)] = canonicalRoots.isEmpty ? nil : Set(canonicalRoots)
        }
    }

    public func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        try Self.validateScope(scope)

        try lock.withWriteLock {
            for root in canonicalRoots {
                try Self.validateRetainedVolume(root: root, state: state)
            }
            if !canonicalRoots.isEmpty {
                state.retainedRoots[Array(scope.utf8), default: []].formUnion(canonicalRoots)
            }
        }
    }

    public func retainedRoots(scope: String) async throws -> [String] {
        guard !scope.isEmpty else { return [] }
        return lock.withReadLock {
            Array(state.retainedRoots[Array(scope.utf8)] ?? []).sorted()
        }
    }

    @discardableResult
    public func sweep() async throws -> Int {
        lock.withWriteLock {
            let retained = Set(state.retainedRoots.values.joined())
            let dead = state.membersByRoot.keys.filter { !retained.contains($0) }
            for root in dead {
                Self.removeVolume(root: root, state: &state)
            }
            return dead.count
        }
    }

    @discardableResult
    public func releaseRetainedRoots(scope: String, roots: [String]) async throws -> Int {
        let canonicalRoots = try Self.canonicalRetainedRoots(roots)
        try Self.validateScope(scope)

        return lock.withWriteLock {
            let key = Array(scope.utf8)
            if var retained = state.retainedRoots[key] {
                retained.subtract(canonicalRoots)
                state.retainedRoots[key] = retained.isEmpty ? nil : retained
            }
            let dead = canonicalRoots.filter { root in
                state.membersByRoot[root] != nil && !state.retainedRoots.values.contains { $0.contains(root) }
            }
            for root in dead {
                Self.removeVolume(root: root, state: &state)
            }
            return dead.count
        }
    }

    public func retainedScopes(prefix: String) async throws -> [String] {
        lock.withReadLock {
            state.retainedRoots
                .filter { !$0.value.isEmpty && $0.key.starts(with: prefix.utf8) }
                .keys
                .sorted { $0.lexicographicallyPrecedes($1) }
                .map { String(decoding: $0, as: UTF8.self) }
        }
    }

    private static func validateScope(_ scope: String) throws {
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }
        // SQLite reads a bound string up to its first NUL, so such a name
        // would stand for another scope. By its bytes: a NUL under a
        // combining mark is one Character that is not "\0".
        guard !scope.utf8.contains(0) else {
            throw BrokerError.invalidRetainedRoots("scope must not contain NUL")
        }
    }

    private static func canonicalRetainedRoots(_ roots: [String]) throws -> [String] {
        let unique = Array(Set(roots))
        if unique.contains(where: { $0.isEmpty }) {
            throw BrokerError.invalidRetainedRoots("roots must not contain empty strings")
        }
        // SQLite reads a bound string up to its first NUL, so such a name
        // would stand for another.
        if unique.contains(where: { $0.utf8.contains(0) }) {
            throw BrokerError.invalidRetainedRoots("roots must not contain NUL")
        }
        return unique.sorted()
    }

    private static func validateRetainedVolume(root: String, state: State) throws {
        guard isComplete(root: root, state: state) else {
            throw BrokerError.missingRetainedRoot(root)
        }
    }
}
