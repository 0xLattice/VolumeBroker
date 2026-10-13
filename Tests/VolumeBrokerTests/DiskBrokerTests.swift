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

@Suite("DiskBroker")
struct DiskBrokerTests {

    private actor StartBarrier {
        private var remaining: Int
        private var waiters: [CheckedContinuation<Void, Never>] = []

        init(participants: Int) {
            remaining = participants
        }

        func wait() async {
            remaining -= 1
            guard remaining > 0 else {
                let waiting = waiters
                waiters.removeAll()
                for waiter in waiting { waiter.resume() }
                return
            }
            await withCheckedContinuation { waiters.append($0) }
        }
    }

    private struct StoreAdvanceObservation: Equatable {
        let volumeExists: Bool
        let retained: Bool
        let advanceSucceeded: Bool
    }

    private struct SweepAdvanceObservation: Equatable {
        let swept: Int
        let volumeExists: Bool
        let retained: Bool
        let advanceSucceeded: Bool
    }

    private static func capture<Value: Sendable>(
        _ operation: @escaping @Sendable () async throws -> Value
    ) async -> Result<Value, BrokerError> {
        do {
            return .success(try await operation())
        } catch let error as BrokerError {
            return .failure(error)
        } catch {
            return .failure(.sqlFailed("unexpected test error: \(error)"))
        }
    }

    private static func race<Left: Sendable, Right: Sendable>(
        _ left: @escaping @Sendable () async -> Left,
        _ right: @escaping @Sendable () async -> Right
    ) async -> (Left, Right) {
        let barrier = StartBarrier(participants: 2)
        async let leftResult: Left = {
            await barrier.wait()
            return await left()
        }()
        async let rightResult: Right = {
            await barrier.wait()
            return await right()
        }()
        return await (leftResult, rightResult)
    }

    private func tempDB() throws -> DiskBroker {
        let path = NSTemporaryDirectory() + "vb_test_\(UUID().uuidString).sqlite"
        return try DiskBroker(path: path)
    }

    private func temporaryDatabase() throws -> (directory: URL, path: String) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerDisk-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return (directory, directory.appendingPathComponent("volumes.sqlite").path)
    }

    @Test func batchPointReadSpansBoundedSQLiteChunks() async throws {
        let broker = try tempDB()
        let rootData = Data("batch-root".utf8)
        let root = cid(for: rootData)
        var entries = [root: rootData]
        for index in 0..<1_200 {
            let data = Data("sparse-entry-\(index)".utf8)
            entries[cid(for: data)] = data
        }
        try await broker.store(volume: SerializedVolume(root: root, entries: entries))

        var requested = Set(entries.keys)
        requested.insert("missing")
        let found = await broker.fetchDataLocal(cids: requested)

        #expect(found == entries)
    }

    private func databaseHealth(at path: String) throws -> (integrity: String, foreignKeyViolations: Int) {
        var database: OpaquePointer?
        guard sqlite3_open_v2(path, &database, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK,
              let database else {
            throw BrokerError.openFailed("health-check open failed")
        }
        defer { sqlite3_close(database) }

        func text(_ sql: String) throws -> String {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
                  let statement,
                  sqlite3_step(statement) == SQLITE_ROW,
                  let value = sqlite3_column_text(statement, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
            }
            return String(cString: value)
        }

        func count(_ sql: String) throws -> Int {
            var statement: OpaquePointer?
            defer { sqlite3_finalize(statement) }
            guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK,
                  let statement,
                  sqlite3_step(statement) == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(database)))
            }
            return Int(sqlite3_column_int64(statement, 0))
        }

        return (
            try text("PRAGMA integrity_check"),
            try count("SELECT COUNT(*) FROM pragma_foreign_key_check")
        )
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func payload(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var encodedEntries = Dictionary(uniqueKeysWithValues: entries.values.map { data in
            (cid(for: data), data)
        })
        encodedEntries[cid(for: rootData)] = rootData
        return SerializedVolume(root: cid(for: rootData), entries: encodedEntries)
    }

    @Test func storeAndFetch() async throws {
        let broker = try tempDB()
        let p = payload("r1", ["c1": Data([1, 2, 3]), "c2": Data([4, 5])])
        try await broker.store(volume: p)

        #expect(await broker.hasVolume(root: p.root))
        let fetched = await broker.fetchVolumeLocal(root: p.root)
        #expect(fetched?.entries.count == 3)
        #expect(fetched?.entries[cid(for: Data([1, 2, 3]))] == Data([1, 2, 3]))
    }

    // MARK: - Extracted-layer boundaries

    /// CASVolumeStore boundary: a stored volume round-trips byte-for-byte.
    @Test func casStoreRoundTrip() async throws {
        let broker = try tempDB()
        let entries = ["a": Data([0, 1, 2, 3]), "b": Data(repeating: 7, count: 256)]
        let stored = payload("round", entries)
        try await broker.store(volume: stored)

        let fetched = await broker.fetchVolumeLocal(root: stored.root)
        #expect(fetched?.entries == stored.entries)
    }

    @Test func sharedCIDSurvivesPartialSweep() async throws {
        let broker = try tempDB()
        let shared = Data([99])
        let r1 = cid("r1")
        try await broker.store(volume: payload("r1", ["shared": shared, "only1": Data([1])]))
        try await broker.store(volume: payload("r2", ["shared": shared, "only2": Data([2])]))
        try await broker.advanceRetainedRoots(scope: "chain-a", roots: [r1])

        #expect(try await broker.sweep() == 1)
        let fetched = await broker.fetchVolumeLocal(root: r1)
        #expect(fetched?.entries[cid(for: shared)] == shared)
    }

    @Test func retainedRootServesAndSurvivesSweep() async throws {
        let broker = try tempDB()
        let keep = cid("keep")
        let drop = cid("drop")
        try await broker.store(volume: payload("keep"))
        try await broker.store(volume: payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [keep])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [keep])
        let evicted = try await broker.sweep()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: keep))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    @Test func retainedRootAdvanceReplacesOnlyItsScope() async throws {
        let broker = try tempDB()
        let old = cid("old")
        let new = cid("new")
        let other = cid("other")
        try await broker.store(volume: payload("old"))
        try await broker.store(volume: payload("new"))
        try await broker.store(volume: payload("other"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.advanceRetainedRoots(scope: "chain-b:state", roots: [other])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [new])
        #expect(try await broker.retainedRoots(scope: "chain-b:state") == [other])
        let evicted = try await broker.sweep()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old) == false)
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: other))
    }

    @Test func retainedRootMergeAddsWithoutReplacingScope() async throws {
        let broker = try tempDB()
        let old = cid("old")
        let new = cid("new")
        let drop = cid("drop")
        try await broker.store(volume: payload("old"))
        try await broker.store(volume: payload("new"))
        try await broker.store(volume: payload("drop"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [old])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])
        try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [new])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [new, old].sorted())
        let evicted = try await broker.sweep()
        #expect(evicted == 1)
        #expect(await broker.hasVolume(root: old))
        #expect(await broker.hasVolume(root: new))
        #expect(await broker.hasVolume(root: drop) == false)
    }

    /// What a node reads at start: the scopes a previous process left, and
    /// only those, with a release it committed still in force.
    @Test func scopesAndTheirListingSurviveReopen() async throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        do {
            let broker = try DiskBroker(path: location.path)
            try await broker.storeVolumesLocal([payload("one"), payload("two"), payload("three")])
            try await broker.mergeRetainedRoots(scope: "chain:body:one", roots: [cid("one")])
            try await broker.mergeRetainedRoots(scope: "chain:body:two", roots: [cid("two")])
            try await broker.mergeRetainedRoots(scope: "chain:body:three", roots: [cid("three")])
            try await broker.releaseRetainedRoots(scope: "chain:body:two", roots: [cid("two")])
        }

        let reopened = try DiskBroker(path: location.path)
        #expect(try await reopened.retainedScopes(prefix: "chain:body:") == ["chain:body:one", "chain:body:three"])
        #expect(try await reopened.retainedRoots(scope: "chain:body:one") == [cid("one")])
        #expect(await reopened.hasVolume(root: cid("two")) == false)
        #expect(await reopened.hasVolume(root: cid("three")))
    }

    /// A listing is one snapshot: with a writer that always leaves one of
    /// two scopes retaining something, no listing sees neither.
    @Test func aListingUnderAWriterIsOneCommittedState() async throws {
        let broker = try tempDB()
        try await broker.store(volume: payload("held"))
        try await broker.mergeRetainedRoots(scope: "p:b", roots: [cid("held")])
        let held = cid("held")
        let writer = Task {
            for _ in 0..<400 {
                try await broker.mergeRetainedRoots(scope: "p:y", roots: [held])
                try await broker.releaseRetainedRoots(scope: "p:b", roots: [held])
                try await broker.mergeRetainedRoots(scope: "p:b", roots: [held])
                try await broker.releaseRetainedRoots(scope: "p:y", roots: [held])
            }
        }
        var listings = 0
        while !writer.isCancelled, listings < 4_000 {
            let scopes = try await broker.retainedScopes(prefix: "p:")
            #expect(!scopes.isEmpty)
            #expect(scopes.allSatisfy { $0 == "p:b" || $0 == "p:y" })
            listings += 1
        }
        try await writer.value
        #expect(await broker.hasVolume(root: held))
    }

    @Test func persistedRetentionSurvivesReopenThenControlsSweep() async throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let retained = cid("retained")
        let unprotected = cid("unprotected")
        let missing = cid("missing")

        do {
            let broker = try DiskBroker(path: location.path)
            try await broker.storeVolumesLocal([payload("retained"), payload("unprotected")])
            try await broker.advanceRetainedRoots(scope: "canonical", roots: [retained])

            await #expect(throws: BrokerError.missingRetainedRoot(missing)) {
                try await broker.advanceRetainedRoots(
                    scope: "canonical",
                    roots: [unprotected, missing]
                )
            }
            await #expect(throws: BrokerError.missingRetainedRoot(missing)) {
                try await broker.mergeRetainedRoots(
                    scope: "canonical",
                    roots: [unprotected, missing]
                )
            }
            #expect(try await broker.retainedRoots(scope: "canonical") == [retained])
        }

        let reopened = try DiskBroker(path: location.path)
        #expect(try await reopened.retainedRoots(scope: "canonical") == [retained])

        #expect(try await reopened.sweep() == 1)
        #expect(await reopened.hasVolume(root: retained))
        #expect(await reopened.hasVolume(root: unprotected) == false)

        try await reopened.advanceRetainedRoots(scope: "canonical", roots: [])
        #expect(try await reopened.sweep() == 1)
        #expect(await reopened.hasVolume(root: retained) == false)

        let health = try databaseHealth(at: location.path)
        #expect(health.integrity == "ok")
        #expect(health.foreignKeyViolations == 0)
    }

    @Test func retainedRootMergeRequiresStoredRoots() async throws {
        let broker = try tempDB()
        let missing = cid("missing")

        do {
            try await broker.mergeRetainedRoots(scope: "chain-a:state", roots: [missing])
            #expect(Bool(false), "merging a retained root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == missing)
        }
        #expect(try await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootReplaceIsNaturallyIdempotent() async throws {
        let broker = try tempDB()
        let r1 = cid("r1")
        try await broker.store(volume: payload("r1"))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])
        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [r1])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [r1])
    }

    @Test func retainedRootAdvanceRequiresStoredRoots() async throws {
        let broker = try tempDB()
        let missing = cid("missing")

        do {
            try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [missing])
            #expect(Bool(false), "retaining a root before storing it must fail")
        } catch BrokerError.missingRetainedRoot(let root) {
            #expect(root == missing)
        }
        #expect(try await broker.retainedRoots(scope: "chain-a:state").isEmpty)
    }

    @Test func retainedRootAdvanceValidatesOnlyRequestedVolume() async throws {
        let broker = try tempDB()
        let root = cid("root")
        try await broker.store(volume: payload("root", [
            "root": Data("root".utf8),
            "child": Data("child".utf8),
        ]))

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [root])

        #expect(try await broker.retainedRoots(scope: "chain-a:state") == [root])
    }

    @Test func retainedRootDoesNotRetainMemberVolume() async throws {
        let broker = try tempDB()
        let object = cid("object")
        let child = cid("child")
        let drop = cid("drop")
        let leaf = cid("leaf")
        try await broker.storeVolumesLocal([
            payload("object", ["object": Data("object".utf8), "child": Data("child".utf8)]),
            payload("child", ["child": Data("child".utf8), "leaf": Data("leaf".utf8)]),
            payload("drop")
        ])

        try await broker.advanceRetainedRoots(scope: "chain-a:state", roots: [object])
        let evicted = try await broker.sweep()

        #expect(evicted == 2)
        #expect(await broker.hasVolume(root: object))
        #expect(await broker.hasVolume(root: child) == false)
        #expect(await broker.hasVolume(root: drop) == false)
        // The member's own bytes are an entry of the retained Volume; what
        // was under it is not.
        #expect(await broker.fetchDataLocal(cid: child) == Data("child".utf8))
        #expect(await broker.fetchDataLocal(cid: leaf) == nil)
    }

    @Test func simultaneousStartSharedStateLinearizability() async throws {
        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volume = payload("store-advance")
            let scope = "shared-scope"

            let (store, advance) = await Self.race(
                { await Self.capture { try await broker.store(volume: volume); return true } },
                { await Self.capture { try await broker.advanceRetainedRoots(scope: scope, roots: [volume.root]); return true } }
            )
            _ = try store.get()
            let advanceSucceeded: Bool
            switch advance {
            case .success:
                advanceSucceeded = true
            case .failure(.missingRetainedRoot):
                advanceSucceeded = false
            case .failure(let error):
                throw error
            }

            let observed = StoreAdvanceObservation(
                volumeExists: await broker.hasVolume(root: volume.root),
                retained: try await broker.retainedRoots(scope: scope).contains(volume.root),
                advanceSucceeded: advanceSucceeded
            )
            let serialOutcomes = [
                StoreAdvanceObservation(volumeExists: true, retained: true, advanceSucceeded: true),
                StoreAdvanceObservation(volumeExists: true, retained: false, advanceSucceeded: false),
            ]
            #expect(serialOutcomes.contains(observed), "store/advance observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }

        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volumes = ["retained-a", "retained-b", "retained-c"].map { payload($0) }
            let scope = "shared-scope"
            try await broker.storeVolumesLocal(volumes)
            try await broker.advanceRetainedRoots(scope: scope, roots: [volumes[0].root])

            let (replace, merge) = await Self.race(
                { await Self.capture { try await broker.advanceRetainedRoots(scope: scope, roots: [volumes[1].root]); return true } },
                { await Self.capture { try await broker.mergeRetainedRoots(scope: scope, roots: [volumes[2].root]); return true } }
            )
            _ = try replace.get()
            _ = try merge.get()

            let observed = Set(try await broker.retainedRoots(scope: scope))
            let serialOutcomes: [Set<String>] = [
                [volumes[1].root],
                [volumes[1].root, volumes[2].root],
            ]
            #expect(serialOutcomes.contains(observed), "replace/merge observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }

        do {
            let location = try temporaryDatabase()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let broker = try DiskBroker(path: location.path)
            let volume = payload("sweep-advance")
            let scope = "shared-scope"
            try await broker.store(volume: volume)

            let (sweep, advance) = await Self.race(
                { await Self.capture { try await broker.sweep() } },
                { await Self.capture { try await broker.advanceRetainedRoots(scope: scope, roots: [volume.root]); return true } }
            )
            let swept = try sweep.get()
            let advanceSucceeded: Bool
            switch advance {
            case .success:
                advanceSucceeded = true
            case .failure(.missingRetainedRoot):
                advanceSucceeded = false
            case .failure(let error):
                throw error
            }

            let observed = SweepAdvanceObservation(
                swept: swept,
                volumeExists: await broker.hasVolume(root: volume.root),
                retained: try await broker.retainedRoots(scope: scope).contains(volume.root),
                advanceSucceeded: advanceSucceeded
            )
            let serialOutcomes = [
                SweepAdvanceObservation(swept: 0, volumeExists: true, retained: true, advanceSucceeded: true),
                SweepAdvanceObservation(swept: 1, volumeExists: false, retained: false, advanceSucceeded: false),
            ]
            #expect(serialOutcomes.contains(observed), "sweep/advance observed: \(observed)")
            let health = try databaseHealth(at: location.path)
            #expect(health.integrity == "ok" && health.foreignKeyViolations == 0)
        }
    }

    /// Regression: DiskBroker is shared across multiple ChainNetwork actors.
    /// Without serialising complete write transactions, two actors calling
    /// store(volume:) calls can concurrently pass SQLITE_OPEN_FULLMUTEX's
    /// per-call serialisation and then both issue BEGIN IMMEDIATE, causing the
    /// second to fail with "cannot start a transaction within a transaction".
    @Test("Concurrent writes from multiple actors do not produce nested-transaction errors")
    func testConcurrentWritesFromMultipleActors() async throws {
        let broker = try tempDB()

        // Simulate multiple ChainNetwork actors sharing the same DiskBroker.
        // Each actor calls store(volume:) concurrently; the write executor
        // must serialise transactions so none overlap.
        let writeCount = 20
        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<writeCount {
                group.addTask {
                    let p = self.payload("root-\(i)", ["cid-\(i)": Data("data-\(i)".utf8)])
                    try await broker.store(volume: p)
                }
            }
            try await group.waitForAll()
        }

        // All writes must have committed — no "nested transaction" errors swallowed.
        for i in 0..<writeCount {
            let hasVolume = await broker.hasVolume(root: cid("root-\(i)"))
            #expect(hasVolume, "root-\(i) missing — write was lost")
        }
    }

    /// Same scenario for the batched write path used during block storage.
    @Test("Concurrent storeBatch calls do not produce nested-transaction errors")
    func testConcurrentStoreBatch() async throws {
        let broker = try tempDB()

        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<20 {
                group.addTask {
                    let payloads = (0..<5).map { j in
                        self.payload("batch-\(i)-\(j)", ["c-\(i)-\(j)": Data()])
                    }
                    try await broker.storeVolumesLocal(payloads)
                }
            }
            try await group.waitForAll()
        }

        for i in 0..<20 {
            for j in 0..<5 {
                let hasVolume = await broker.hasVolume(root: cid("batch-\(i)-\(j)"))
                #expect(hasVolume, "batch-\(i)-\(j) missing")
            }
        }
    }
}
