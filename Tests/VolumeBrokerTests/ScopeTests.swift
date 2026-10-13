import Testing
import Foundation
import CID
import Multihash
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif
@testable import VolumeBroker

/// Scopes as a chain node keeps them: one long-lived chain scope, two
/// mempool scopes, and one scope per block body.
@Suite("Scopes")
struct ScopeTests {

    enum Kind: String, CaseIterable, Sendable {
        case memory
        case disk
    }

    struct Store: Sendable {
        let broker: any RetainedRootReleaseBroker
        let path: String?
    }

    static func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    static func cid(_ label: String) -> String { cid(for: Data(label.utf8)) }

    static func volume(_ label: String, _ members: [String] = []) -> SerializedVolume {
        var entries = [cid(label): Data(label.utf8)]
        for member in members { entries[cid(member)] = Data(member.utf8) }
        return SerializedVolume(root: cid(label), entries: entries)
    }

    static func open(_ kind: Kind) throws -> Store {
        switch kind {
        case .memory:
            return Store(broker: MemoryBroker(), path: nil)
        case .disk:
            let path = NSTemporaryDirectory() + "vb_scopeprobe_\(UUID().uuidString).sqlite"
            return Store(broker: try DiskBroker(path: path), path: path)
        }
    }

    /// A fresh store has no -wal/-shm when `SQLiteConnection` opens its
    /// read-only connection, so that open fails and reads fall back to the
    /// write connection. One opened before (where the sidecars outlive a
    /// close, as with Apple's SQLite) gets the read connection.
    enum Opened: String, CaseIterable, Sendable { case fresh, before }

    static func openDisk(_ opened: Opened) async throws -> Store {
        let path = NSTemporaryDirectory() + "vb_scopeprobe_\(UUID().uuidString).sqlite"
        if opened == .before {
            let first = try DiskBroker(path: path)
            try await first.storeVolumesLocal([volume("opened-before")])
            try await first.sweep()
        }
        return Store(broker: try DiskBroker(path: path), path: path)
    }

    // MARK: - Raw SQL (a second connection, read-only unless said)

    static func rows(_ path: String, _ sql: String, write: Bool = false) -> [[String]] {
        var db: OpaquePointer?
        guard sqlite3_open_v2(path, &db, write ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY, nil) == SQLITE_OK,
              let db else { return [["open failed"]] }
        defer { sqlite3_close(db) }
        sqlite3_busy_timeout(db, 30_000)
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            return [["prepare failed: \(String(cString: sqlite3_errmsg(db)))"]]
        }
        var out: [[String]] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            out.append((0..<sqlite3_column_count(stmt)).map { i in
                sqlite3_column_text(stmt, i).map { String(cString: $0) } ?? "NULL"
            })
        }
        return out
    }

    static func count(_ path: String, _ sql: String) -> Int {
        Int(rows(path, sql).first?.first ?? "") ?? -1
    }

    /// What must hold of a disk store at rest, whatever the scopes are.
    static func diskProblems(_ path: String, allowUnretained: Bool = false) -> [String] {
        var problems: [String] = []
        if rows(path, "PRAGMA integrity_check").first?.first != "ok" { problems.append("integrity_check") }
        let checks: [(String, String)] = [
            ("foreign_key_check", "SELECT COUNT(*) FROM pragma_foreign_key_check"),
            ("a retained root that is not a complete Volume", """
                SELECT COUNT(*) FROM retained_roots rr WHERE NOT EXISTS (
                    SELECT 1 FROM volume_metadata vm WHERE vm.root = rr.root AND \(CASVolumeStore.completeVolumePredicate))
                """),
            ("a CAS row no Volume owns", """
                SELECT COUNT(*) FROM cas_data WHERE NOT EXISTS (
                    SELECT 1 FROM volume_entries ve WHERE ve.cid = cas_data.cid)
                """),
            ("an incomplete Volume", """
                SELECT COUNT(*) FROM volume_metadata vm WHERE NOT (\(CASVolumeStore.completeVolumePredicate))
                """),
        ]
        for (name, sql) in checks where count(path, sql) != 0 {
            problems.append("\(name): \(count(path, sql))")
        }
        let unretained = count(path, "SELECT COUNT(*) FROM volume_metadata WHERE root NOT IN (SELECT root FROM retained_roots)")
        if !allowUnretained, unretained != 0 { problems.append("a stored Volume no scope retains: \(unretained)") }
        return problems
    }

    static func tableCounts(_ path: String) -> [Int] {
        ["volume_metadata", "volume_entries", "cas_data", "retained_roots"].map {
            count(path, "SELECT COUNT(*) FROM \($0)")
        }
    }

    /// Through the public surface only, so both brokers: every Volume of
    /// `universe` is present exactly when some scope names it, is served
    /// whole when present, and a CID is served exactly when a present
    /// Volume owns it.
    static func apiProblems(
        _ broker: any RetainedRootReleaseBroker, universe: [SerializedVolume], exact: Bool = true
    ) async throws -> [String] {
        var problems: [String] = []
        var named = Set<String>()
        for scope in try await broker.retainedScopes(prefix: "") {
            let roots = try await broker.retainedRoots(scope: scope)
            if roots.isEmpty { problems.append("listed scope retains nothing: \(scope)") }
            named.formUnion(roots)
        }
        var owned = Set<String>()
        for volume in universe {
            let present = await broker.hasVolume(root: volume.root)
            if named.contains(volume.root), !present { problems.append("retained but absent: \(volume.root)") }
            if exact, present, !named.contains(volume.root) { problems.append("present but unretained: \(volume.root)") }
            if present {
                if await broker.fetchVolumeLocal(root: volume.root)?.entries != volume.entries {
                    problems.append("present but not whole: \(volume.root)")
                }
                owned.formUnion(volume.entries.keys)
            }
        }
        let all = Set(universe.flatMap(\.entries.keys))
        let served = Set(await broker.fetchDataLocal(cids: all).keys)
        if served != owned { problems.append("served CIDs differ from owned: +\(served.subtracting(owned).count) -\(owned.subtracting(served).count)") }
        return problems
    }

    static func permutations<T>(_ items: [T]) -> [[T]] {
        guard items.count > 1 else { return [items] }
        return items.indices.flatMap { i -> [[T]] in
            var rest = items
            let head = rest.remove(at: i)
            return permutations(rest).map { [head] + $0 }
        }
    }

    // MARK: - 1. One root in the chain scope, a body scope and a mempool scope

    @Test(arguments: Kind.allCases)
    func aRootThreeScopesNameGoesWithTheLastInEveryOrder(kind: Kind) async throws {
        let chain = "bafychain:Nexus"
        let body = "\(chain):body:\(Self.cid("block"))"
        let mempool = "\(chain):live-mempool"
        let universe = [Self.volume("tx", ["tx-leaf"]), Self.volume("state"), Self.volume("block", ["tx"])]
        let also = [chain: Self.cid("state"), body: Self.cid("block")]

        for order in Self.permutations([chain, body, mempool]) {
            let store = try Self.open(kind)
            let broker = store.broker
            try await broker.storeVolumesLocal(universe)
            for scope in [chain, body, mempool] {
                try await broker.mergeRetainedRoots(scope: scope, roots: [Self.cid("tx")] + (also[scope].map { [$0] } ?? []))
            }
            var left = Set([chain, body, mempool])
            for scope in order {
                let removed = try await broker.releaseRetainedRoots(scope: scope, roots: [Self.cid("tx")])
                left.remove(scope)
                #expect(removed == (left.isEmpty ? 1 : 0), "\(order) at \(scope)")
                #expect(await broker.hasVolume(root: Self.cid("tx")) == !left.isEmpty, "\(order) at \(scope)")
                #expect((await broker.fetchDataLocal(cid: Self.cid("tx-leaf")) != nil) == !left.isEmpty, "\(order) at \(scope)")
                #expect(try await broker.retainedRoots(scope: scope).contains(Self.cid("tx")) == false)
                for other in left {
                    #expect(try await broker.retainedRoots(scope: other).contains(Self.cid("tx")), "\(order) at \(scope): \(other)")
                }
                // The mempool scope held only the transaction: it is gone when released.
                let listed = try await broker.retainedScopes(prefix: "")
                #expect(Set(listed) == Set([chain, body] + (left.contains(mempool) ? [mempool] : [])), "\(order) at \(scope)")
            }
            // The transaction's bytes are still the block's own entry.
            #expect(await broker.fetchDataLocal(cid: Self.cid("tx")) == Data("tx".utf8))
            #expect(await broker.hasVolume(root: Self.cid("block")))
            #expect(await broker.hasVolume(root: Self.cid("state")))
            // Gone is gone: no scope can name it again until it is stored again.
            for scope in [chain, body, mempool, "\(chain):body:new"] {
                await #expect(throws: BrokerError.missingRetainedRoot(Self.cid("tx"))) {
                    try await broker.mergeRetainedRoots(scope: scope, roots: [Self.cid("tx")])
                }
            }
            #expect(try await Self.apiProblems(broker, universe: universe) == [])
            if let path = store.path { #expect(Self.diskProblems(path) == []) }
        }
    }

    // MARK: - 2. Naming a root that was released and deleted fails closed

    @Test(arguments: Kind.allCases)
    func namingAReleasedAndDeletedRootInANewScopeFailsClosed(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let universe = [Self.volume("tx", ["tx-leaf"]), Self.volume("block")]
        try await broker.storeVolumesLocal(universe)
        try await broker.mergeRetainedRoots(scope: "c:body:a", roots: [Self.cid("tx")])
        #expect(try await broker.releaseRetainedRoots(scope: "c:body:a", roots: [Self.cid("tx")]) == 1)

        // All-or-none: the block that is present is not retained either.
        await #expect(throws: BrokerError.missingRetainedRoot(Self.cid("tx"))) {
            try await broker.mergeRetainedRoots(scope: "c:body:b", roots: [Self.cid("block"), Self.cid("tx")])
        }
        await #expect(throws: BrokerError.missingRetainedRoot(Self.cid("tx"))) {
            try await broker.advanceRetainedRoots(scope: "c:body:b", roots: [Self.cid("block"), Self.cid("tx")])
        }
        #expect(try await broker.retainedScopes(prefix: "") == [])
        #expect(try await broker.retainedRoots(scope: "c:body:b") == [])
        #expect(await broker.hasVolume(root: Self.cid("tx")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("tx-leaf")) == nil)

        // Stored again, it is retained whole.
        try await broker.storeVolumesLocal(universe)
        try await broker.mergeRetainedRoots(scope: "c:body:b", roots: [Self.cid("block"), Self.cid("tx")])
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        if let path = store.path { #expect(Self.diskProblems(path) == []) }
    }

    // MARK: - 3. Store-then-merge against a release, every serial order

    /// A body fetch stores a block and a transaction the store already has
    /// (a sibling's scope names it), then merges both into the new block's
    /// scope. The sibling is released in between.
    @Test(arguments: Kind.allCases)
    func aReleaseBetweenAStoreAndItsMergeFailsTheMergeAndARetryRestores(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let universe = [Self.volume("tx", ["tx-leaf"]), Self.volume("block-a"), Self.volume("block-b")]
        try await broker.storeVolumesLocal([universe[0], universe[1]])
        try await broker.mergeRetainedRoots(scope: "c:body:a", roots: ["block-a", "tx"].map(Self.cid))

        try await broker.storeVolumesLocal([universe[2], universe[0]])
        #expect(try await broker.releaseRetainedRoots(
            scope: "c:body:a", roots: try await broker.retainedRoots(scope: "c:body:a")) == 2)
        await #expect(throws: BrokerError.missingRetainedRoot(Self.cid("tx"))) {
            try await broker.mergeRetainedRoots(scope: "c:body:b", roots: ["block-b", "tx"].map(Self.cid))
        }
        // Nothing is retained that is not there, and the new scope does not exist.
        #expect(try await broker.retainedScopes(prefix: "c:body:") == [])
        // The block the failed merge did not retain is stored and unnamed:
        // a listing does not find it, a sweep or a retry does.
        #expect(await broker.hasVolume(root: Self.cid("block-b")))
        #expect(try await Self.apiProblems(broker, universe: universe, exact: false) == [])

        try await broker.storeVolumesLocal([universe[2], universe[0]])
        try await broker.mergeRetainedRoots(scope: "c:body:b", roots: ["block-b", "tx"].map(Self.cid))
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        if let path = store.path { #expect(Self.diskProblems(path) == []) }
    }

    // MARK: - 4. Roots that are entries of other Volumes

    @Test(arguments: Kind.allCases)
    func releasingARootAnotherRetainedVolumeHoldsAsAnEntryKeepsThatVolumeWhole(kind: Kind) async throws {
        // The block's Volume holds the transaction's root bytes as an entry;
        // the transaction is also a Volume of its own. One scope names both.
        let universe = [Self.volume("block", ["tx", "block-leaf"]), Self.volume("tx", ["tx-leaf", "block-leaf"])]
        for first in ["tx", "block"] {
            let second = first == "tx" ? "block" : "tx"
            let store = try Self.open(kind)
            let broker = store.broker
            try await broker.storeVolumesLocal(universe)
            try await broker.mergeRetainedRoots(scope: "c:body:x", roots: ["block", "tx"].map(Self.cid))

            #expect(try await broker.releaseRetainedRoots(scope: "c:body:x", roots: [Self.cid(first)]) == 1)
            #expect(await broker.hasVolume(root: Self.cid(first)) == false)
            #expect(await broker.fetchVolumeLocal(root: Self.cid(second))?.entries
                == universe.first { $0.root == Self.cid(second) }!.entries, "after \(first)")
            #expect(await broker.fetchDataLocal(cid: Self.cid("block-leaf")) == Data("block-leaf".utf8))
            // The bytes of a released root stay while a surviving Volume holds them as an entry.
            #expect(await broker.fetchDataLocal(cid: Self.cid("tx")) == Data("tx".utf8))
            #expect((await broker.fetchDataLocal(cid: Self.cid("block")) != nil) == (first == "tx"))
            #expect(await broker.fetchDataLocal(cid: Self.cid("tx-leaf")) == (first == "tx" ? nil : Data("tx-leaf".utf8)))
            #expect(try await Self.apiProblems(broker, universe: universe) == [])
            if let path = store.path { #expect(Self.diskProblems(path) == []) }

            #expect(try await broker.releaseRetainedRoots(scope: "c:body:x", roots: [Self.cid(second)]) == 1)
            #expect(await broker.fetchDataLocal(cids: Set(universe.flatMap(\.entries.keys))) == [:])
            if let path = store.path { #expect(Self.tableCounts(path) == [0, 0, 0, 0]) }
        }
    }

    /// Both released in one call, as a node releases a whole body scope.
    @Test(arguments: Kind.allCases)
    func aWholeScopeWithANestedRootIsReleasedToNothing(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let universe = [
            Self.volume("block", ["tx-1", "tx-2", "block-leaf"]),
            Self.volume("tx-1", ["shared-leaf"]), Self.volume("tx-2", ["shared-leaf"]),
            Self.volume("state", ["shared-leaf"]),
        ]
        try await broker.storeVolumesLocal(universe)
        try await broker.mergeRetainedRoots(scope: "c", roots: [Self.cid("state")])
        try await broker.mergeRetainedRoots(scope: "c:body:x", roots: ["block", "tx-1", "tx-2"].map(Self.cid))
        #expect(try await broker.releaseRetainedRoots(
            scope: "c:body:x", roots: try await broker.retainedRoots(scope: "c:body:x")) == 3)
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        #expect(await broker.fetchDataLocal(cid: Self.cid("shared-leaf")) == Data("shared-leaf".utf8))
        if let path = store.path {
            #expect(Self.diskProblems(path) == [])
            #expect(Self.tableCounts(path) == [1, 2, 2, 1])
        }
    }

    // MARK: - 5. A body with thousands of transactions

    @Test(arguments: Kind.allCases)
    func aScopeOfThousandsOfRootsIsReleasedWholeInOneCall(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let txs = (0..<4_000).map { Self.volume("tx-\($0)", ["tx-\($0)-leaf", "every-tx-leaf"]) }
        let state = (0..<4_000).map { Self.volume("state-\($0)") }
        let block = Self.volume("block", ["block-leaf"])
        try await broker.storeVolumesLocal(txs + state + [block])
        try await broker.advanceRetainedRoots(scope: "c", roots: state.map(\.root))
        let body = "c:body:\(block.root)"
        try await broker.mergeRetainedRoots(scope: body, roots: txs.map(\.root) + [block.root])
        // A fork sibling carries every fourth transaction; the mempool every tenth.
        let sibling = txs.enumerated().filter { $0.offset % 4 == 0 }.map(\.element.root)
        let pooled = txs.enumerated().filter { $0.offset % 10 == 0 }.map(\.element.root)
        try await broker.mergeRetainedRoots(scope: "c:body:sibling", roots: sibling)
        try await broker.mergeRetainedRoots(scope: "c:live-mempool", roots: pooled)
        let surviving = Set(sibling).union(pooled)

        let roots = try await broker.retainedRoots(scope: body)
        #expect(roots.count == 4_001)
        let removed = try await broker.releaseRetainedRoots(scope: body, roots: roots)
        #expect(removed == 4_001 - surviving.count)
        #expect(try await broker.retainedScopes(prefix: "c:body:") == ["c:body:sibling"])
        #expect(try await broker.retainedRoots(scope: "c").count == 4_000)
        #expect(try await Self.apiProblems(broker, universe: txs + state + [block]) == [])
        #expect(await broker.fetchDataLocal(cid: Self.cid("every-tx-leaf")) != nil)
        if let path = store.path {
            #expect(Self.diskProblems(path) == [])
            // 4000 state + surviving txs; each surviving tx owns root + leaf, and they share one leaf.
            #expect(Self.tableCounts(path) == [
                4_000 + surviving.count, 4_000 + 3 * surviving.count, 4_000 + 2 * surviving.count + 1,
                4_000 + sibling.count + pooled.count,
            ])
        }
    }

    // MARK: - 6. Emptying a scope by advance is not a release

    @Test(arguments: Kind.allCases)
    func aScopeEmptiedByAdvanceLeavesItsVolumesForASweepOrANamedRelease(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let universe = [Self.volume("block", ["block-leaf"]), Self.volume("tx"), Self.volume("state")]
        try await broker.storeVolumesLocal(universe)
        try await broker.mergeRetainedRoots(scope: "c", roots: [Self.cid("state")])
        try await broker.mergeRetainedRoots(scope: "c:body:x", roots: ["block", "tx"].map(Self.cid))

        try await broker.advanceRetainedRoots(scope: "c:body:x", roots: [])
        // The scope is gone from the listing and names nothing ...
        #expect(try await broker.retainedScopes(prefix: "c:body:") == [])
        #expect(try await broker.retainedRoots(scope: "c:body:x") == [])
        // ... but nothing was reclaimed: read-then-release now finds nothing to release.
        #expect(await broker.hasVolume(root: Self.cid("block")))
        #expect(await broker.hasVolume(root: Self.cid("tx")))
        #expect(try await broker.releaseRetainedRoots(
            scope: "c:body:x", roots: try await broker.retainedRoots(scope: "c:body:x")) == 0)
        #expect(await broker.hasVolume(root: Self.cid("block")))

        // A caller that still knows the roots can release them by name, from any scope ...
        #expect(try await broker.releaseRetainedRoots(scope: "c:body:x", roots: [Self.cid("block")]) == 1)
        #expect(await broker.fetchDataLocal(cid: Self.cid("block-leaf")) == nil)
        // ... and a sweep takes the rest.
        #expect(try await broker.sweep() == 1)
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        if let path = store.path { #expect(Self.diskProblems(path) == []) }
    }

    // MARK: - 7. A sweep beside body scopes

    @Test(arguments: Kind.allCases)
    func aSweepKeepsWhatBodyScopesNameAndTakesAStoreNotYetMerged(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let universe = [
            Self.volume("state"), Self.volume("block-a", ["tx"]), Self.volume("tx", ["tx-leaf"]),
            Self.volume("pooled"), Self.volume("block-b", ["tx", "b-leaf"]),
        ]
        try await broker.storeVolumesLocal(universe)
        try await broker.mergeRetainedRoots(scope: "c", roots: [Self.cid("state")])
        try await broker.mergeRetainedRoots(scope: "c:body:a", roots: ["block-a", "tx"].map(Self.cid))
        try await broker.mergeRetainedRoots(scope: "c:durable-mempool", roots: [Self.cid("pooled")])
        // block-b is stored; its merge has not run yet.

        #expect(try await broker.sweep() == 1)
        #expect(try await broker.retainedScopes(prefix: "") == ["c", "c:body:a", "c:durable-mempool"])
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        #expect(await broker.fetchDataLocal(cid: Self.cid("b-leaf")) == nil)
        await #expect(throws: BrokerError.missingRetainedRoot(Self.cid("block-b"))) {
            try await broker.mergeRetainedRoots(scope: "c:body:b", roots: ["block-b", "tx"].map(Self.cid))
        }
        #expect(try await broker.retainedScopes(prefix: "c:body:") == ["c:body:a"])
        if let path = store.path { #expect(Self.diskProblems(path) == []) }
    }

    // MARK: - 8. A store from before bodies had scopes

    @Test(arguments: Kind.allCases)
    func aLegacyStoreHasNoBodyScopesAndMovesToThemWithoutLoss(kind: Kind) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let chain = "bafychain:Nexus"
        let universe = [Self.volume("state"), Self.volume("block-1", ["tx-1"]), Self.volume("tx-1", ["leaf"]),
                        Self.volume("block-2", ["tx-1"])]
        try await broker.storeVolumesLocal(universe)
        try await broker.advanceRetainedRoots(scope: chain, roots: universe.map(\.root))

        // Start-up cleanup finds nothing and so releases nothing.
        #expect(try await broker.retainedScopes(prefix: "\(chain):body:") == [])
        // The chain's own name is not a body scope, nor is a mempool scope.
        try await broker.mergeRetainedRoots(scope: "\(chain):live-mempool", roots: [Self.cid("tx-1")])
        #expect(try await broker.retainedScopes(prefix: "\(chain):body:") == [])

        // The new version gives block-1 its scope (the store is a no-op), then drops the chain's hold.
        try await broker.storeVolumesLocal([universe[1], universe[2]])
        try await broker.mergeRetainedRoots(scope: "\(chain):body:1", roots: ["block-1", "tx-1"].map(Self.cid))
        #expect(try await broker.releaseRetainedRoots(scope: chain, roots: ["block-1", "tx-1", "block-2"].map(Self.cid)) == 1)
        #expect(await broker.hasVolume(root: Self.cid("block-2")) == false)
        #expect(try await broker.retainedRoots(scope: chain) == [Self.cid("state")])
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        // Letting go of the body: the mempool still names the transaction.
        #expect(try await broker.releaseRetainedRoots(
            scope: "\(chain):body:1", roots: try await broker.retainedRoots(scope: "\(chain):body:1")) == 1)
        #expect(await broker.hasVolume(root: Self.cid("tx-1")))
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        if let path = store.path { #expect(Self.diskProblems(path) == []) }
    }

    // MARK: - 9. NUL in an argument, every entry point

    /// A scope name with a NUL can never have been written, so reading one
    /// answers for no scope, on both brokers alike.
    @Test(arguments: Kind.allCases)
    func aNULInAReadArgumentStandsForNoOtherScope(kind: Kind) async throws {
        let broker = try Self.open(kind).broker
        try await broker.store(volume: Self.volume("kept"))
        try await broker.mergeRetainedRoots(scope: "a", roots: [Self.cid("kept")])
        try await broker.mergeRetainedRoots(scope: "ab", roots: [Self.cid("kept")])

        #expect(try await broker.retainedRoots(scope: "a\0b") == [], "retainedRoots(scope: a\\0b)")
        #expect(try await broker.retainedScopes(prefix: "a\0b") == [], "retainedScopes(prefix: a\\0b)")
        #expect(try await broker.retainedScopes(prefix: "\0") == [], "retainedScopes(prefix: \\0)")
    }

    /// U+0000 followed by a combining mark is one Character that is not "\0".
    @Test(arguments: Kind.allCases)
    func aNULUnderACombiningMarkIsStillRefused(kind: Kind) async throws {
        let broker = try Self.open(kind).broker
        try await broker.store(volume: Self.volume("kept"))
        try await broker.mergeRetainedRoots(scope: "a", roots: [Self.cid("kept")])
        let disguised = "a\0\u{301}"
        #expect(disguised.utf8.contains(0))

        await #expect(throws: BrokerError.self) {
            try await broker.releaseRetainedRoots(scope: disguised, roots: [Self.cid("kept")])
        }
        #expect(try await broker.retainedRoots(scope: "a") == [Self.cid("kept")], "scope a lost its root to a\\0\\u{301}")
        #expect(await broker.hasVolume(root: Self.cid("kept")), "kept was deleted through a\\0\\u{301}")
        await #expect(throws: BrokerError.self) {
            try await broker.advanceRetainedRoots(scope: disguised, roots: [])
        }
        #expect(try await broker.retainedRoots(scope: "a") == [Self.cid("kept")], "scope a emptied through a\\0\\u{301}")
        await #expect(throws: BrokerError.self) {
            try await broker.mergeRetainedRoots(scope: "b\0\u{301}", roots: [Self.cid("kept")])
        }
        #expect(try await broker.retainedScopes(prefix: "").map { Array($0.utf8) } == [Array("a".utf8)])
        // The same disguise in a root name.
        await #expect(throws: BrokerError.self) {
            try await broker.releaseRetainedRoots(scope: "a", roots: [Self.cid("kept") + "\0\u{301}"])
        }
        #expect(await broker.hasVolume(root: Self.cid("kept")), "kept was deleted through a disguised root name")
    }

    // MARK: - 10. The listing against a model, every prefix

    static let names: [String] = [
        "a", "a:", "a:b", "a:b:c", "a;", "b", "~", "A", "a%", "a_", "a:%", "a:_x", "a\u{7F}",
        "caf\u{E9}", "cafe\u{301}", "caff", "a:\u{10FFFF}", "a:\u{10FFFF}z", "\u{10FFFF}", "\u{1}", " ",
        "a:" + String(repeating: "z", count: 300), "bafyreichain:Nexus:body:" + String(repeating: "b", count: 59),
        "bodyguard", "body", "body:", "body:x",
    ]

    static func model(_ names: [String], prefix: String) -> [[UInt8]] {
        names.map { Array($0.utf8) }.filter { $0.starts(with: prefix.utf8) }
            .sorted { $0.lexicographicallyPrecedes($1) }
    }

    @Test(arguments: Kind.allCases)
    func theListingMatchesAByteModelForEveryPrefix(kind: Kind) async throws {
        let broker = try Self.open(kind).broker
        try await broker.store(volume: Self.volume("kept"))
        var prefixes = Set(["", "zzz", "\u{10FFFF}\u{10FFFF}", "a:b:c:d", "%", "_", "a:%", "\u{80}", "body:y", "c"])
        for name in Self.names {
            for end in name.unicodeScalars.indices { prefixes.insert(String(name.unicodeScalars[..<end])) }
            prefixes.insert(name)
            prefixes.insert(name + "!")
        }
        // An empty table first.
        for prefix in prefixes {
            #expect(try await broker.retainedScopes(prefix: prefix) == [])
        }
        // Then grown one scope at a time, in an order that is not sorted.
        var present: [String] = []
        for name in Self.names.reversed() {
            try await broker.mergeRetainedRoots(scope: name, roots: [Self.cid("kept")])
            present.append(name)
            guard present.count % 5 == 0 || present.count == Self.names.count else { continue }
            for prefix in prefixes {
                let listed = try await broker.retainedScopes(prefix: prefix).map { Array($0.utf8) }
                #expect(listed == Self.model(present, prefix: prefix), "prefix \(Array(prefix.utf8)) with \(present.count) scopes")
            }
        }
        // Then shrunk from the middle.
        for name in Self.names where name.utf8.count % 2 == 0 {
            try await broker.releaseRetainedRoots(scope: name, roots: [Self.cid("kept")])
            present.removeAll { $0.utf8.elementsEqual(name.utf8) }
        }
        for prefix in prefixes {
            let listed = try await broker.retainedScopes(prefix: prefix).map { Array($0.utf8) }
            #expect(listed == Self.model(present, prefix: prefix), "after releases, prefix \(Array(prefix.utf8))")
        }
    }

    static let listingSQL: String = {
        let starts = "substr(CAST(scope AS BLOB), 1, length(CAST(?1 AS BLOB))) = CAST(?1 AS BLOB)"
        return """
            WITH RECURSIVE found(scope) AS (
                SELECT min(scope) FROM retained_roots WHERE scope >= ?1
                UNION ALL
                SELECT (SELECT min(scope) FROM retained_roots WHERE scope > found.scope)
                FROM found WHERE scope IS NOT NULL AND \(starts)
            )
            SELECT scope FROM found WHERE scope IS NOT NULL AND \(starts)
            """
    }()

    /// A hundred thousand body scopes beside a chain scope of as many roots,
    /// written straight into the table (retained_roots has no foreign key).
    /// What makes the listing cost the scopes listed is its plan, which is
    /// pinned here; timings are a measurement and not asserted.
    @Test func manyScopesAreListedByIndexSeeksAlone() async throws {
        let path = NSTemporaryDirectory() + "vb_scopeprobe_million_\(UUID().uuidString).sqlite"
        defer { for suffix in ["", "-wal", "-shm"] { try? FileManager.default.removeItem(atPath: path + suffix) } }
        let broker = try DiskBroker(path: path)
        let fill = """
            WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 99999)
            INSERT INTO retained_roots(scope, root)
            SELECT 'bafychain:Nexus:body:bafyrei' || printf('%052d', (i * 7919) % 100000), 'bafyreiroot' || i FROM n
            UNION ALL SELECT 'bafychain:Nexus:body:bafyrei' || printf('%052d', (i * 7919) % 100000), 'bafyreitx' || i FROM n WHERE i % 2 = 0
            UNION ALL SELECT 'bafychain:Nexus', 'bafyreistate' || i FROM n
            UNION ALL SELECT 'bafychain:Nexus:live-mempool', 'bafyreipool' || i FROM n WHERE i < 5000
            """
        _ = Self.rows(path, fill, write: true)
        #expect(Self.count(path, "SELECT COUNT(*) FROM retained_roots") == 255_000)

        let all = try await broker.retainedScopes(prefix: "bafychain:Nexus:body:")
        #expect(all.count == 100_000)
        #expect(all.first == "bafychain:Nexus:body:bafyrei" + String(repeating: "0", count: 52))
        #expect(zip(all, all.dropFirst()).allSatisfy { $0.utf8.lexicographicallyPrecedes($1.utf8) })
        let some = try await broker.retainedScopes(
            prefix: "bafychain:Nexus:body:bafyrei" + String(repeating: "0", count: 49))
        #expect(some.count == 1_000)
        #expect(try await broker.retainedScopes(prefix: "bafychain:Nexus:bodz") == [])
        #expect(try await broker.retainedScopes(prefix: "bafychain:Nexus:l") == ["bafychain:Nexus:live-mempool"])

        let plan = Self.rows(path, "EXPLAIN QUERY PLAN " + Self.listingSQL).map { $0.last ?? "" }
        let searches = plan.filter { $0.contains("retained_roots") }
        #expect(searches.count == 2)
        #expect(searches.allSatisfy { $0.hasPrefix("SEARCH retained_roots USING COVERING INDEX sqlite_autoindex_retained_roots_1 (scope>?)") })
        #expect(!plan.contains { $0.contains("SCAN retained_roots") })

        // Releasing one body scope in a store this size is a matter of its own roots.
        let removed = try await broker.releaseRetainedRoots(
            scope: all[50_000], roots: try await broker.retainedRoots(scope: all[50_000]))
        #expect(removed == 0)
        #expect(try await broker.retainedScopes(prefix: all[50_000]) == [])
    }

    /// One commit changes one scope, so a listing that is short cannot tell
    /// one snapshot from several reads. A long one can: the writer never
    /// commits a state without the first scope or the last.
    @Test(arguments: Opened.allCases)
    func aLongListingUnderAWriterIsOneCommittedState(opened: Opened) async throws {
        let store = try await Self.openDisk(opened)
        let broker = store.broker
        let held = Self.cid("held")
        try await broker.store(volume: Self.volume("held"))
        try await broker.mergeRetainedRoots(scope: "p:!first", roots: [held])
        _ = Self.rows(store.path!, """
            WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < 19999)
            INSERT INTO retained_roots(scope, root) SELECT 'p:body:' || printf('%06d', i), '\(held)' FROM n
            """, write: true)
        let stop = Ledger()
        let writer = Task {
            var rounds = 0
            while await stop.merged.isEmpty {
                // {first} {first,last} {last} {first,last} {first}
                try await broker.mergeRetainedRoots(scope: "p:~last", roots: [held])
                try await broker.releaseRetainedRoots(scope: "p:!first", roots: [held])
                try await broker.mergeRetainedRoots(scope: "p:!first", roots: [held])
                try await broker.releaseRetainedRoots(scope: "p:~last", roots: [held])
                rounds += 1
            }
            return rounds
        }
        var torn = 0
        for _ in 0..<40 {
            let scopes = try await broker.retainedScopes(prefix: "p:")
            let body = scopes.filter { $0.hasPrefix("p:body:") }.count
            if body != 20_000 || !(scopes.first == "p:!first" || scopes.last == "p:~last") {
                torn += 1
            }
        }
        await stop.add("stop")
        let rounds = try await writer.value
        #expect(torn == 0, "\(torn) of 40 listings were no committed state")
        #expect(rounds >= 40, "the writer committed too rarely to tear a listing: \(rounds) rounds")
        #expect(await broker.hasVolume(root: held))
    }

    static func readOnlyOpen(_ path: String) -> String {
        var db: OpaquePointer?
        guard sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK, let db else { return "open failed" }
        defer { sqlite3_close(db) }
        var out: [String] = []
        for sql in ["PRAGMA foreign_keys=ON", "PRAGMA foreign_keys", "PRAGMA busy_timeout=5000", "PRAGMA cache_size=-16384"] {
            let rc = sqlite3_exec(db, sql, nil, nil, nil)
            if rc != SQLITE_OK { out.append("\(sql) -> \(rc) \(String(cString: sqlite3_errmsg(db)))") }
        }
        return out.isEmpty ? "ok" : out.joined(separator: "; ")
    }

    static func sidecars(_ path: String) -> String {
        ["-wal", "-shm"].map { "\($0) \(FileManager.default.fileExists(atPath: path + $0) ? "exists" : "absent")" }.joined(separator: ", ")
    }

    /// Reads are documented to run on a separate read-only WAL connection.
    @Test func readsRunOnTheirOwnConnection() async throws {
        let path = NSTemporaryDirectory() + "vb_scopeprobe_conn_\(UUID().uuidString).sqlite"
        do {
            let connection = try SQLiteConnection(path: path)
            #expect(connection.readDb != connection.db, "fresh store")
            let store = CASVolumeStore(connection: connection)
            try await store.storeVolumesLocal([Self.volume("one")])
        }
        do {
            let connection = try SQLiteConnection(path: path)
            #expect(connection.readDb != connection.db, "reopened store")
        }
    }

    /// Whatever connection reads, a read must not see a transaction that has not committed.
    @Test(arguments: Opened.allCases)
    func aReadNeverSeesAnAdvanceHalfDone(opened: Opened) async throws {
        let store = try await Self.openDisk(opened)
        let broker = store.broker
        let volumes = (0..<3_000).map { Self.volume("v\($0)") }
        try await broker.storeVolumesLocal(volumes)
        let all = volumes.map(\.root), half = Array(all.prefix(1_500))
        try await broker.advanceRetainedRoots(scope: "c", roots: all)
        let stop = Ledger()
        let writer = Task {
            while await stop.merged.isEmpty {
                try await broker.advanceRetainedRoots(scope: "c", roots: half)
                try await broker.advanceRetainedRoots(scope: "c", roots: all)
            }
        }
        var seen: [Int: Int] = [:]
        var unlisted = 0
        for _ in 0..<3_000 {
            seen[try await broker.retainedRoots(scope: "c").count, default: 0] += 1
            if try await broker.retainedScopes(prefix: "c") != ["c"] { unlisted += 1 }
        }
        await stop.add("stop")
        try await writer.value
        let dirty = seen.filter { $0.key != 3_000 && $0.key != 1_500 }
        #expect(dirty.isEmpty)
        #expect(unlisted == 0)
    }

    // MARK: - 11. Concurrency: fetches, releases, a mempool and a lister on one broker

    actor Ledger {
        var merged: [String] = []
        var failures: [String] = []
        var retries = 0
        func add(_ scope: String) { merged.append(scope) }
        func take() -> String? { merged.isEmpty ? nil : merged.removeFirst() }
        func fail(_ message: String) { failures.append(message) }
        func retried() { retries += 1 }
        var finishedFetchers = 0
        func fetcherFinished() { finishedFetchers += 1 }
    }

    /// Store then merge, as a body fetch does; a merge refused because a
    /// release removed something between the two is retried from the store.
    static func storeThenMerge(
        _ broker: any RetainedRootReleaseBroker, scope: String, volumes: [SerializedVolume], ledger: Ledger
    ) async {
        for _ in 0..<1_000 {
            do {
                try await broker.storeVolumesLocal(volumes)
                await Task.yield()
                try await broker.mergeRetainedRoots(scope: scope, roots: volumes.map(\.root))
                return
            } catch BrokerError.missingRetainedRoot {
                await ledger.retried()
            } catch {
                await ledger.fail("\(scope): \(error)")
                return
            }
        }
        await ledger.fail("\(scope): never merged")
    }

    @Test(arguments: [(Kind.memory, false), (.disk, false), (.memory, true), (.disk, true)])
    func fetchesReleasesAMempoolAndAListerLeaveTheStoreExactlyAsTheScopesSay(kind: Kind, sweeping: Bool) async throws {
        let store = try Self.open(kind)
        let broker = store.broker
        let chain = "bafychain:Nexus"
        let bodyPrefix = "\(chain):body:"
        let fetchers = 6, blocksEach = kind == .disk ? 12 : 60, releasers = 3
        let shared = Self.volume("shared-tx", ["shared-leaf"])
        let state = Self.volume("state", ["shared-leaf"])
        func blockVolumes(_ f: Int, _ b: Int) -> [SerializedVolume] {
            [Self.volume("block-\(f)-\(b)", ["shared-tx", "block-\(f)-\(b)-leaf"]),
             Self.volume("tx-\(f)-\(b)", ["every-leaf"]),
             // A transaction two fork siblings carry.
             Self.volume("pair-tx-\(b)-\(f / 2)", ["every-leaf"]),
             shared]
        }
        let universe = [state] + (0..<fetchers).flatMap { f in (0..<blocksEach).flatMap { blockVolumes(f, $0) } }
        try await broker.storeVolumesLocal([state])
        try await broker.mergeRetainedRoots(scope: chain, roots: [state.root])
        let ledger = Ledger()
        let done = Ledger()

        try await withThrowingTaskGroup(of: Void.self) { group in
            for f in 0..<fetchers {
                group.addTask {
                    for b in 0..<blocksEach {
                        let volumes = blockVolumes(f, b)
                        let scope = bodyPrefix + volumes[0].root
                        await Self.storeThenMerge(broker, scope: scope, volumes: volumes, ledger: ledger)
                        // Every third body is kept; the rest are let go by another task.
                        if b % 3 != 0 { await ledger.add(scope) }
                    }
                    await ledger.fetcherFinished()
                }
            }
            for _ in 0..<releasers {
                group.addTask {
                    while true {
                        guard let scope = await ledger.take() else {
                            if await ledger.finishedFetchers == fetchers { break }
                            try await Task.sleep(for: .milliseconds(1)); continue
                        }
                        let roots = try await broker.retainedRoots(scope: scope)
                        if roots.count != 4 { await ledger.fail("\(scope) retained \(roots.count) before its release") }
                        try await broker.releaseRetainedRoots(scope: scope, roots: roots)
                    }
                }
            }
            // A mempool that admits and drops the shared transaction.
            group.addTask {
                var round = 0
                while await done.merged.isEmpty {
                    await Self.storeThenMerge(broker, scope: "\(chain):live-mempool", volumes: [shared], ledger: ledger)
                    try await broker.releaseRetainedRoots(scope: "\(chain):live-mempool", roots: [shared.root])
                    round += 1
                    try await Task.sleep(for: .microseconds(200))
                }
            }
            // A lister: sorted, only body scopes, each once.
            group.addTask {
                while await done.merged.isEmpty {
                    let listed = try await broker.retainedScopes(prefix: bodyPrefix)
                    if !listed.allSatisfy({ $0.hasPrefix(bodyPrefix) && $0.utf8.count == bodyPrefix.utf8.count + 59 }) {
                        await ledger.fail("listing held a name that is not a body scope")
                    }
                    if !zip(listed, listed.dropFirst()).allSatisfy({ $0.utf8.lexicographicallyPrecedes($1.utf8) }) {
                        await ledger.fail("listing not strictly ascending")
                    }
                    #expect(try await broker.retainedScopes(prefix: chain).first == chain)
                    try await Task.sleep(for: .milliseconds(1))
                }
            }
            if sweeping {
                group.addTask {
                    while await done.merged.isEmpty {
                        try await broker.sweep()
                        try await Task.sleep(for: .milliseconds(25))
                    }
                }
            }
            // Stop the mempool, the lister and the sweeper once every fetch is merged and every release has run.
            group.addTask {
                // No wall-clock limit: under a sanitizer or on a loaded runner
                // the same work takes many times longer, and that is no failure.
                while true {
                    let finished = await ledger.finishedFetchers
                    let queued = await ledger.merged.count
                    if finished == fetchers, queued == 0 { break }
                    try await Task.sleep(for: .milliseconds(5))
                }
                await done.add("done")
            }
            try await group.waitForAll()
        }
        let failures = await ledger.failures
        #expect(failures.isEmpty, "\(failures.prefix(5))")

        // Exactly consistent with the scopes: the kept bodies whole, nothing else.
        let keptScopes = try await broker.retainedScopes(prefix: bodyPrefix)
        #expect(keptScopes.count == fetchers * ((blocksEach + 2) / 3))
        for scope in keptScopes {
            #expect(try await broker.retainedRoots(scope: scope).count == 4)
        }
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        #expect(await broker.hasVolume(root: shared.root))
        if let path = store.path { #expect(Self.diskProblems(path) == []) }

        // Then every body is let go: the shared transaction goes with the last.
        for (index, scope) in keptScopes.enumerated() {
            #expect(await broker.hasVolume(root: shared.root))
            try await broker.releaseRetainedRoots(scope: scope, roots: try await broker.retainedRoots(scope: scope))
            #expect(await broker.hasVolume(root: shared.root) == (index < keptScopes.count - 1))
        }
        #expect(try await broker.retainedScopes(prefix: "") == [chain])
        #expect(try await Self.apiProblems(broker, universe: universe) == [])
        #expect(await broker.fetchDataLocal(cid: Self.cid("shared-leaf")) == Data("shared-leaf".utf8))
        #expect(await broker.fetchDataLocal(cid: Self.cid("every-leaf")) == nil)
        if let path = store.path {
            #expect(Self.diskProblems(path) == [])
            #expect(Self.tableCounts(path) == [1, 2, 2, 1])
        }
    }
}
