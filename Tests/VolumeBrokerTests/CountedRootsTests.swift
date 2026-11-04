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

/// Counted roots and their markers, run against both brokers where the
/// behaviour is shared, and against the SQLite store for schema and reopen.
@Suite("Counted roots")
struct CountedRootsTests {

    enum Kind: String, CaseIterable, Sendable {
        case memory
        case disk
    }

    private static func cid(_ label: String) -> String {
        let multihash = try! Multihash(raw: Data(label.utf8), hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    /// A Volume rooted at `label` whose entries also hold `members`.
    private static func volume(_ label: String, _ members: [String] = []) -> SerializedVolume {
        var entries = [cid(label): Data(label.utf8)]
        for member in members { entries[cid(member)] = Data(member.utf8) }
        return SerializedVolume(root: cid(label), entries: entries)
    }

    private static func path() -> String {
        NSTemporaryDirectory() + "vb_counted_\(UUID().uuidString).sqlite"
    }

    private static func broker(_ kind: Kind) throws -> any CountedRootBroker {
        switch kind {
        case .memory: return MemoryBroker()
        case .disk: return try DiskBroker(path: path())
        }
    }

    private static func counts(_ pairs: [String: Int]) -> [String: Int] {
        Dictionary(uniqueKeysWithValues: pairs.map { (cid($0.key), $0.value) })
    }

    // MARK: - Shared behaviour

    @Test(arguments: Kind.allCases)
    func countsAccumulateAndTheMarkerMoves(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a"), Self.volume("b")])

        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1, "b": 2]), marker: "m1", expectedMarker: nil)
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1, "b": -1]), marker: "m2", expectedMarker: "m1")

        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": 2, "b": 1]))
        #expect(try await broker.countedState(scope: "s").marker == "m2")
        #expect(try await broker.countedState(scope: "other").marker == nil)
        #expect(try await broker.countedState(scope: "other").counts == [:])
        // Counted scopes are not retained scopes.
        #expect(try await broker.retainedScopes(prefix: "") == [])
        #expect(try await broker.retainedRoots(scope: "s") == [])
    }

    @Test(arguments: Kind.allCases)
    func aMarkerMismatchChangesNothing(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a"), Self.volume("b")])

        await #expect(throws: BrokerError.countedMarkerMismatch(expected: "m0", found: nil)) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": 1]), marker: "m1", expectedMarker: "m0")
        }
        #expect(try await broker.countedScopes(prefix: "") == [])

        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1]), marker: "m1", expectedMarker: nil)
        await #expect(throws: BrokerError.countedMarkerMismatch(expected: nil, found: "m1")) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": 1, "b": 1]), marker: "m2", expectedMarker: nil)
        }
        await #expect(throws: BrokerError.countedMarkerMismatch(expected: "m9", found: "m1")) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": -1]), marker: "m2", expectedMarker: "m9")
        }
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": 1]))
        #expect(try await broker.countedState(scope: "s").marker == "m1")
        #expect(await broker.hasVolume(root: Self.cid("a")))
    }

    @Test(arguments: Kind.allCases)
    func aNegativeCountChangesNothing(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a"), Self.volume("b"), Self.volume("c")])
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1, "b": 1]), marker: "m1", expectedMarker: nil)

        await #expect(throws: BrokerError.negativeCount(Self.cid("b"))) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": -1, "b": -2, "c": 1]), marker: "m2", expectedMarker: "m1")
        }
        await #expect(throws: BrokerError.negativeCount(Self.cid("c"))) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["c": -1]), marker: "m2", expectedMarker: "m1")
        }
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": 1, "b": 1]))
        #expect(try await broker.countedState(scope: "s").marker == "m1")
        // `a` would have reached zero: it was not reclaimed.
        #expect(await broker.hasVolume(root: Self.cid("a")))
    }

    @Test(arguments: Kind.allCases)
    func startingToCountAMissingRootListsEveryMissingRootAndChangesNothing(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a"), Self.volume("b")])
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["b": 1]), marker: "m1", expectedMarker: nil)

        let missing = ["x", "y"].map(Self.cid).sorted()
        await #expect(throws: BrokerError.countedRootsMissing(missing)) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": 1, "b": -1, "x": 1, "y": 3]),
                marker: "m2", expectedMarker: "m1")
        }
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["b": 1]))
        #expect(try await broker.countedState(scope: "s").marker == "m1")
        #expect(await broker.hasVolume(root: Self.cid("b")))

        // Stored, the same adjustment goes through.
        try await broker.storeVolumesLocal([Self.volume("x"), Self.volume("y")])
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1, "b": -1, "x": 1, "y": 3]),
            marker: "m2", expectedMarker: "m1")
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": 1, "x": 1, "y": 3]))
    }

    @Test(arguments: Kind.allCases)
    func zeroDeltasAreIgnored(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        // `never` is not stored: a zero delta does not ask for it.
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1, "never": 0]), marker: "m1", expectedMarker: nil)
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 0]), marker: "m2", expectedMarker: "m1")
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": 1]))
        #expect(try await broker.countedState(scope: "s").marker == "m2")
    }

    @Test(arguments: Kind.allCases)
    func reachingZeroReclaimsOnlyWhatNothingElseKeeps(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([
            Self.volume("alone", ["alone-leaf", "shared-leaf"]),
            Self.volume("retained", ["retained-leaf"]),
            Self.volume("twice", ["twice-leaf"]),
            Self.volume("kept", ["shared-leaf"]),
        ])
        try await broker.adjustCountedRoots(
            scope: "c", deltas: Self.counts(["alone": 1, "retained": 1, "twice": 1, "kept": 1]),
            marker: "m1", expectedMarker: nil)
        try await broker.advanceRetainedRoots(scope: "r", roots: [Self.cid("retained")])
        try await broker.adjustCountedRoots(
            scope: "c2", deltas: Self.counts(["twice": 2]), marker: "n1", expectedMarker: nil)

        try await broker.adjustCountedRoots(
            scope: "c", deltas: Self.counts(["alone": -1, "retained": -1, "twice": -1]),
            marker: "m2", expectedMarker: "m1")

        #expect(try await broker.countedState(scope: "c").counts == Self.counts(["kept": 1]))
        #expect(await broker.hasVolume(root: Self.cid("alone")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("alone-leaf")) == nil)
        // Bytes a surviving Volume owns stay.
        #expect(await broker.fetchDataLocal(cid: Self.cid("shared-leaf")) == Data("shared-leaf".utf8))
        #expect(await broker.hasVolume(root: Self.cid("retained")))
        #expect(await broker.hasVolume(root: Self.cid("twice")))
        #expect(await broker.hasVolume(root: Self.cid("kept")))

        // The last count goes: now nothing keeps it.
        try await broker.adjustCountedRoots(
            scope: "c2", deltas: Self.counts(["twice": -2]), marker: "n2", expectedMarker: "n1")
        #expect(await broker.hasVolume(root: Self.cid("twice")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("twice-leaf")) == nil)
        #expect(try await broker.countedState(scope: "c2").counts == [:])
        // A scope with a marker and no counts is still a scope.
        #expect(try await broker.countedScopes(prefix: "") == ["c", "c2"])
    }

    @Test(arguments: Kind.allCases)
    func aCountedRootSurvivesReleaseAndSweep(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("counted", ["leaf"]), Self.volume("stray")])
        try await broker.advanceRetainedRoots(scope: "r", roots: [Self.cid("counted")])
        try await broker.adjustCountedRoots(
            scope: "c", deltas: Self.counts(["counted": 1]), marker: "m1", expectedMarker: nil)

        #expect(try await broker.releaseRetainedRoots(scope: "r", roots: [Self.cid("counted")]) == 0)
        #expect(await broker.hasVolume(root: Self.cid("counted")))
        #expect(try await broker.sweep() == 1)
        #expect(await broker.hasVolume(root: Self.cid("counted")))
        #expect(await broker.fetchDataLocal(cid: Self.cid("leaf")) == Data("leaf".utf8))
        #expect(await broker.hasVolume(root: Self.cid("stray")) == false)
    }

    @Test(arguments: Kind.allCases)
    func droppingAScopeReclaimsWhatOnlyItCounted(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([
            Self.volume("alone", ["alone-leaf"]),
            Self.volume("shared"),
            Self.volume("retained"),
            Self.volume("stray"),
        ])
        try await broker.adjustCountedRoots(
            scope: "c1", deltas: Self.counts(["alone": 3, "shared": 1, "retained": 1]),
            marker: "m1", expectedMarker: nil)
        try await broker.adjustCountedRoots(
            scope: "c2", deltas: Self.counts(["shared": 1]), marker: "n1", expectedMarker: nil)
        try await broker.advanceRetainedRoots(scope: "r", roots: [Self.cid("retained")])

        try await broker.dropCountedScope(scope: "c1")

        #expect(try await broker.countedState(scope: "c1").counts == [:])
        #expect(try await broker.countedState(scope: "c1").marker == nil)
        #expect(try await broker.countedScopes(prefix: "") == ["c2"])
        #expect(await broker.hasVolume(root: Self.cid("alone")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("alone-leaf")) == nil)
        #expect(await broker.hasVolume(root: Self.cid("shared")))
        #expect(await broker.hasVolume(root: Self.cid("retained")))
        // Nothing it did not count is touched.
        #expect(await broker.hasVolume(root: Self.cid("stray")))

        // Dropping a scope that does not exist is not an error, and a dropped
        // scope starts again from no marker.
        try await broker.dropCountedScope(scope: "c1")
        try await broker.adjustCountedRoots(
            scope: "c1", deltas: Self.counts(["shared": 1]), marker: "m1", expectedMarker: nil)
        #expect(try await broker.countedState(scope: "c1").counts == Self.counts(["shared": 1]))
    }

    @Test(arguments: Kind.allCases)
    func scopesAreListedByBytePrefix(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        let composed = "caf\u{E9}"
        let decomposed = "cafe\u{301}"
        for scope in ["chain:counted:b", "chain:counted:a", "chain:countedx", "chain:staging:a", "z", composed, decomposed] {
            try await broker.adjustCountedRoots(
                scope: scope, deltas: Self.counts(["a": 1]), marker: "m", expectedMarker: nil)
        }
        // A scope with a marker and nothing counted is listed.
        try await broker.adjustCountedRoots(scope: "chain:counted:%", deltas: [:], marker: "m", expectedMarker: nil)

        #expect(try await broker.countedScopes(prefix: "chain:counted:")
            == ["chain:counted:%", "chain:counted:a", "chain:counted:b"])
        #expect(try await broker.countedScopes(prefix: "chain:counted")
            == ["chain:counted:%", "chain:counted:a", "chain:counted:b", "chain:countedx"])
        #expect(try await broker.countedScopes(prefix: "chain:counted:b") == ["chain:counted:b"])
        #expect(try await broker.countedScopes(prefix: "none") == [])
        #expect(try await broker.countedScopes(prefix: "\0") == [])
        #expect(try await broker.countedScopes(prefix: "").count == 8)
        let caf = try await broker.countedScopes(prefix: "caf")
        #expect(caf.map { Array($0.utf8) } == [Array(decomposed.utf8), Array(composed.utf8)])
        let cafe = try await broker.countedScopes(prefix: "cafe")
        #expect(cafe.map { Array($0.utf8) } == [Array(decomposed.utf8)])
    }

    @Test(arguments: Kind.allCases)
    func malformedNamesAreRefused(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        let a = Self.cid("a")
        let refused: [(scope: String, deltas: [String: Int], marker: String, expected: String?)] = [
            ("", [a: 1], "m", nil),
            ("s\0t", [a: 1], "m", nil),
            ("s", ["": 1], "m", nil),
            ("s", [a + "\0junk": 1], "m", nil),
        ]
        for call in refused {
            await #expect(throws: BrokerError.self) {
                try await broker.adjustCountedRoots(
                    scope: call.scope, deltas: call.deltas, marker: call.marker, expectedMarker: call.expected)
            }
        }
        let badMarkers: [(marker: String, expected: String?)] = [("", nil), ("m\0n", nil), ("m", ""), ("m", "m\0n")]
        for call in badMarkers {
            do {
                try await broker.adjustCountedRoots(
                    scope: "s", deltas: [a: 1], marker: call.marker, expectedMarker: call.expected)
                Issue.record("marker \(call) must be refused")
            } catch BrokerError.invalidCountedMarker {
            } catch {
                Issue.record("marker \(call) refused with \(error)")
            }
        }
        await #expect(throws: BrokerError.self) { try await broker.dropCountedScope(scope: "") }
        await #expect(throws: BrokerError.self) { try await broker.dropCountedScope(scope: "s\0t") }
        #expect(try await broker.countedScopes(prefix: "") == [])
        #expect(try await broker.countedState(scope: "s\0t").counts == [:])
        #expect(try await broker.countedState(scope: "s\0t").marker == nil)
    }

    @Test(arguments: Kind.allCases)
    func aCountPastIntMaxIsRefusedAsOverflow(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": Int.max]), marker: "m1", expectedMarker: nil)
        #expect(try await broker.countedState(scope: "s").counts == Self.counts(["a": Int.max]))
        await #expect(throws: BrokerError.countedRootOverflow(Self.cid("a"))) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": 1]), marker: "m2", expectedMarker: "m1")
        }
        let state = try await broker.countedState(scope: "s")
        #expect(state.marker == "m1")
        #expect(state.counts == Self.counts(["a": Int.max]))
    }

    /// Writers racing from one marker: exactly one wins.
    @Test(arguments: Kind.allCases)
    func concurrentAdjustmentsFromOneMarkerHaveOneWinner(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        let deltas = Self.counts(["a": 1])
        let wins = await withTaskGroup(of: Bool.self) { group in
            for i in 0..<50 {
                group.addTask {
                    (try? await broker.adjustCountedRoots(
                        scope: "s", deltas: deltas, marker: "m\(i)", expectedMarker: nil)) != nil
                }
            }
            var wins = 0
            for await won in group where won { wins += 1 }
            return wins
        }
        #expect(wins == 1)
        #expect(try await broker.countedState(scope: "s").counts == deltas)
    }

    /// Each adjustment sets the marker to the count it leaves: every read,
    /// taken while they run, sees a marker and counts from the same commit.
    @Test(arguments: Kind.allCases)
    func aStateReadSeesTheMarkerWithItsCounts(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("a")])
        let a = Self.cid("a")
        try await broker.adjustCountedRoots(scope: "s", deltas: [a: 1], marker: "1", expectedMarker: nil)
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for count in 2...200 {
                    try await broker.adjustCountedRoots(
                        scope: "s", deltas: [a: 1], marker: "\(count)", expectedMarker: "\(count - 1)")
                }
            }
            for _ in 0..<4 {
                group.addTask {
                    for _ in 0..<200 {
                        let state = try await broker.countedState(scope: "s")
                        #expect(state.marker == state.counts[a].map(String.init))
                    }
                }
            }
            try await group.waitForAll()
        }
        #expect(try await broker.countedState(scope: "s").counts == [a: 200])
    }

    // MARK: - SQLite store

    private enum RawError: Error { case sqlite(String) }

    private static func withDatabase<T>(at path: String, _ body: (OpaquePointer) throws -> T) throws -> T {
        var db: OpaquePointer?
        guard sqlite3_open_v2(path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK,
              let db else {
            if let db { sqlite3_close(db) }
            throw RawError.sqlite("open failed")
        }
        defer { sqlite3_close(db) }
        sqlite3_busy_timeout(db, 5_000)
        return try body(db)
    }

    private static func execute(_ db: OpaquePointer, _ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw RawError.sqlite(String(cString: sqlite3_errmsg(db)))
        }
    }

    private static func scalar(_ db: OpaquePointer, _ sql: String) throws -> Int {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt,
              sqlite3_step(stmt) == SQLITE_ROW else {
            throw RawError.sqlite(String(cString: sqlite3_errmsg(db)))
        }
        return Int(sqlite3_column_int64(stmt, 0))
    }

    @Test func countsMarkersAndScopesSurviveReopen() async throws {
        let path = Self.path()
        do {
            let broker = try DiskBroker(path: path)
            try await broker.storeVolumesLocal([Self.volume("a"), Self.volume("b")])
            try await broker.adjustCountedRoots(
                scope: "x:counted", deltas: Self.counts(["a": 2, "b": 1]), marker: "m1", expectedMarker: nil)
        }
        let reopened = try DiskBroker(path: path)
        #expect(try await reopened.countedState(scope: "x:counted").counts == Self.counts(["a": 2, "b": 1]))
        #expect(try await reopened.countedState(scope: "x:counted").marker == "m1")
        #expect(try await reopened.countedScopes(prefix: "x:") == ["x:counted"])
        #expect(try await reopened.sweep() == 0)
        #expect(await reopened.hasVolume(root: Self.cid("a")))
    }

    /// A store written by a binary that predates counted roots gets the
    /// tables on open, at the same version, with everything else untouched.
    @Test func aStoreWithoutTheTablesGetsThemOnOpenAtTheSameVersion() async throws {
        let path = Self.path()
        do {
            let broker = try DiskBroker(path: path)
            try await broker.storeVolumesLocal([Self.volume("a")])
            try await broker.advanceRetainedRoots(scope: "r", roots: [Self.cid("a")])
        }
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "DROP INDEX idx_counted_roots_root")
            try Self.execute(db, "DROP TABLE counted_roots")
            try Self.execute(db, "DROP TABLE counted_markers")
            #expect(try Self.scalar(db, "SELECT COUNT(*) FROM sqlite_schema WHERE name LIKE '%counted%'") == 0)
        }

        let reopened = try DiskBroker(path: path)
        try Self.withDatabase(at: path) { db throws in
            #expect(try Self.scalar(db, "PRAGMA user_version") == 2)
            #expect(try Self.scalar(db, """
                SELECT COUNT(*) FROM sqlite_schema
                WHERE name IN ('counted_roots', 'counted_markers', 'idx_counted_roots_root')
                """) == 3)
        }
        #expect(try await reopened.retainedRoots(scope: "r") == [Self.cid("a")])
        try await reopened.adjustCountedRoots(
            scope: "c", deltas: Self.counts(["a": 1]), marker: "m1", expectedMarker: nil)
        #expect(try await reopened.countedState(scope: "c").counts == Self.counts(["a": 1]))
    }

    /// The validation of the binary before counted roots, verbatim: the
    /// tables it owns, its indexes, and no foreign key from any other table
    /// into them. A store with counted roots in it passes, so rolling back
    /// still opens it.
    @Test func theValidationBeforeCountedRootsAcceptsTheNewTables() async throws {
        let path = Self.path()
        do {
            let broker = try DiskBroker(path: path)
            try await broker.storeVolumesLocal([Self.volume("a")])
            try await broker.adjustCountedRoots(
                scope: "c", deltas: Self.counts(["a": 1]), marker: "m1", expectedMarker: nil)
        }
        try Self.withDatabase(at: path) { db throws in
            let protectedTables = "'cas_data','volume_metadata','volume_entries','retained_roots'"
            let allowedIndexes = "'idx_ve_cid','idx_ve_root','idx_retained_roots_root'"
            #expect(try Self.scalar(db, "PRAGMA user_version") == 2)
            #expect(try Self.scalar(db, """
                SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name IN (\(protectedTables))
                """) == 0)
            #expect(try Self.scalar(db, """
                SELECT COUNT(*) FROM sqlite_schema
                WHERE type='index'
                  AND tbl_name IN (\(protectedTables))
                  AND sql IS NOT NULL
                  AND name NOT IN (\(allowedIndexes))
                """) == 0)
            #expect(try Self.scalar(db, """
                SELECT COUNT(*)
                FROM sqlite_schema AS child
                JOIN pragma_foreign_key_list(child.name) AS fk
                WHERE child.type='table'
                  AND child.name NOT IN (\(protectedTables))
                  AND fk."table" COLLATE NOCASE IN (\(protectedTables))
                """) == 0)
            #expect(try Self.scalar(db, "SELECT COUNT(*) FROM pragma_foreign_key_check") == 0)
            // No foreign key at all, so none can hold back an older binary's deletes.
            #expect(try Self.scalar(db, "SELECT COUNT(*) FROM pragma_foreign_key_list('counted_roots')") == 0)
            #expect(try Self.scalar(db, "SELECT COUNT(*) FROM pragma_foreign_key_list('counted_markers')") == 0)
        }
    }

    /// A SQL failure late in an adjustment, at the marker write or in the
    /// reclaim's CAS delete, rolls back everything before it.
    @Test func aSQLFailureMidAdjustmentRollsBackEverything() async throws {
        let path = Self.path()
        let broker = try DiskBroker(path: path)
        try await broker.storeVolumesLocal([Self.volume("a", ["leaf"]), Self.volume("b")])
        try await broker.adjustCountedRoots(
            scope: "s", deltas: Self.counts(["a": 1]), marker: "m1", expectedMarker: nil)

        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, """
                CREATE TRIGGER fail_marker BEFORE UPDATE ON counted_markers
                BEGIN SELECT RAISE(ABORT, 'marker'); END
                """)
        }
        await #expect(throws: BrokerError.self) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": -1, "b": 1]), marker: "m2", expectedMarker: "m1")
        }
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "DROP TRIGGER fail_marker")
            try Self.execute(db, """
                CREATE TRIGGER fail_cas BEFORE DELETE ON cas_data
                BEGIN SELECT RAISE(ABORT, 'cas'); END
                """)
        }
        await #expect(throws: BrokerError.self) {
            try await broker.adjustCountedRoots(
                scope: "s", deltas: Self.counts(["a": -1]), marker: "m2", expectedMarker: "m1")
        }
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "DROP TRIGGER fail_cas")
        }

        let state = try await broker.countedState(scope: "s")
        #expect(state.marker == "m1")
        #expect(state.counts == Self.counts(["a": 1]))
        #expect(await broker.hasVolume(root: Self.cid("a")))
        #expect(await broker.fetchDataLocal(cid: Self.cid("leaf")) == Data("leaf".utf8))
    }

    @Test(arguments: [
        "CREATE VIEW counted_markers AS SELECT 1 AS scope, 2 AS marker",
        "CREATE VIEW COUNTED_ROOTS AS SELECT 1 AS scope",
        "CREATE INDEX idx_counted_roots_root ON retained_roots(scope)",
    ])
    func anotherObjectUnderACountedNameIsRejected(sql: String) async throws {
        let path = Self.path()
        _ = try DiskBroker(path: path)
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "DROP INDEX idx_counted_roots_root")
            try Self.execute(db, "DROP TABLE counted_roots")
            try Self.execute(db, "DROP TABLE counted_markers")
            try Self.execute(db, sql)
        }
        #expect(throws: BrokerError.invalidSchema(version: 2)) {
            _ = try DiskBroker(path: path)
        }
    }

    @Test func aBehaviorChangingCountedTableIsRejected() async throws {
        let path = Self.path()
        _ = try DiskBroker(path: path)
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "DROP TABLE counted_markers")
            try Self.execute(db, "CREATE TABLE counted_markers (scope TEXT PRIMARY KEY, marker TEXT)")
        }
        #expect(throws: BrokerError.invalidSchema(version: 2)) {
            _ = try DiskBroker(path: path)
        }
    }

    /// After a rollback, the older binary's sweep, which does not see
    /// counted roots, removes a Volume a scope counts. On the next open that
    /// scope loses its counts and its marker; every other scope stays.
    @Test func aScopeCountingARootAnOlderBinaryRemovedIsDroppedOnOpen() async throws {
        let path = Self.path()
        do {
            let broker = try DiskBroker(path: path)
            try await broker.storeVolumesLocal([
                Self.volume("gone", ["gone-leaf"]), Self.volume("kept"), Self.volume("both"),
            ])
            try await broker.advanceRetainedRoots(scope: "r", roots: [Self.cid("kept"), Self.cid("both")])
            try await broker.adjustCountedRoots(
                scope: "broken", deltas: Self.counts(["gone": 1, "both": 1]), marker: "m1", expectedMarker: nil)
            try await broker.adjustCountedRoots(
                scope: "fine", deltas: Self.counts(["kept": 1]), marker: "n1", expectedMarker: nil)
        }
        // The sweep of the binary before counted roots, verbatim.
        try Self.withDatabase(at: path) { db throws in
            try Self.execute(db, "PRAGMA foreign_keys=ON")
            try Self.execute(db, """
                BEGIN IMMEDIATE;
                DELETE FROM volume_metadata WHERE root NOT IN (SELECT root FROM retained_roots);
                DELETE FROM cas_data WHERE NOT EXISTS (
                    SELECT 1 FROM volume_entries ve WHERE ve.cid = cas_data.cid);
                COMMIT;
                """)
        }

        let reopened = try DiskBroker(path: path)
        #expect(await reopened.hasVolume(root: Self.cid("gone")) == false)
        #expect(try await reopened.countedScopes(prefix: "") == ["fine"])
        #expect(try await reopened.countedState(scope: "broken").counts == [:])
        #expect(try await reopened.countedState(scope: "broken").marker == nil)
        #expect(try await reopened.countedState(scope: "fine").counts == Self.counts(["kept": 1]))
        #expect(try await reopened.countedState(scope: "fine").marker == "n1")
        // The dropped scope starts again from no marker.
        try await reopened.storeVolumesLocal([Self.volume("gone", ["gone-leaf"])])
        try await reopened.adjustCountedRoots(
            scope: "broken", deltas: Self.counts(["gone": 1, "both": 1]), marker: "m1", expectedMarker: nil)
    }
}
