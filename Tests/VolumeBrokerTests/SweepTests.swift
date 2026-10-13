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

/// SQL-level sweep tests over one shared `SQLiteConnection`, without the
/// `DiskBroker` façade: schema-forbidden states, corrupt bytes, and SQL failures.
@Suite("Sweep")
struct SweepTests {

    private struct Harness {
        let connection: SQLiteConnection
        let store: CASVolumeStore
        let retained: RetainedRootIndex
    }

    private func harness() throws -> Harness {
        let path = NSTemporaryDirectory() + "vb_sweep_\(UUID().uuidString).sqlite"
        let connection = try SQLiteConnection(path: path)
        return Harness(
            connection: connection,
            store: CASVolumeStore(connection: connection),
            retained: RetainedRootIndex(connection: connection)
        )
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func volume(_ root: String, _ entries: [String: Data] = [:]) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var encodedEntries = Dictionary(uniqueKeysWithValues: entries.values.map { data in
            (cid(for: data), data)
        })
        encodedEntries[cid(for: rootData)] = rootData
        return SerializedVolume(root: cid(for: rootData), entries: encodedEntries)
    }

    @Test func unretainedRootIsSwept() async throws {
        let h = try harness()
        try await h.store.store(volume: volume("drop"))

        #expect(try await h.retained.sweep() == 1)
        #expect(await h.store.hasVolume(root: cid("drop")) == false)
        #expect(await casRowCount(connection: h.connection, cid: cid("drop")) == 0)
    }

    @Test func retainedRootSurvives() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))
        try await h.retained.advanceRetainedRoots(scope: "s", roots: [root])

        #expect(try await h.retained.sweep() == 0)
        #expect(await h.store.hasVolume(root: root))
    }

    @Test func sharedBlobIsDeletedAfterLastOwner() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        let sharedCID = cid(for: shared)
        let keepRoot = cid("keep")
        try await h.store.store(volume: volume("keep", ["shared": shared]))
        try await h.store.store(volume: volume("drop", ["shared": shared]))
        try await h.retained.advanceRetainedRoots(scope: "s", roots: [keepRoot])

        #expect(try await h.retained.sweep() == 1)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 1)

        try await h.retained.advanceRetainedRoots(scope: "s", roots: [])
        #expect(try await h.retained.sweep() == 1)
        #expect(await casRowCount(connection: h.connection, cid: sharedCID) == 0)
        #expect(await h.store.fetchDataLocal(cid: sharedCID) == nil)
    }

    @Test func retainedIntentSurvivesContentLoss() async throws {
        let h = try harness()
        let root = cid("lost")
        try await h.store.store(volume: volume("lost"))
        try await h.retained.advanceRetainedRoots(scope: "canonical", roots: [root])
        try await h.connection.write {
            try h.connection.exec("DELETE FROM volume_metadata WHERE root='\(root)'")
        }

        #expect(await h.store.hasVolume(root: root) == false)
        #expect(try await h.retained.retainedRoots(scope: "canonical") == [root])
    }

    /// The completeness predicate omits what the schema already enforces.
    /// Each state it relies on being impossible is refused by SQLite.
    @Test func schemaRefusesEveryStateThePredicateDoesNotCheck() async throws {
        let h = try harness()
        let root = cid("r1")
        try await h.store.store(volume: volume("r1"))

        let forbidden = [
            // A member without a CAS row.
            "INSERT INTO volume_entries(root, cid) VALUES('\(root)', 'no-content')",
            // A member of a Volume with no metadata.
            "INSERT INTO volume_entries(root, cid) VALUES('no-metadata', '\(root)')",
            // Removing content a Volume still owns.
            "DELETE FROM cas_data WHERE cid='\(root)'",
            // A non-integer or nonpositive declared count.
            "UPDATE volume_metadata SET entry_count=CAST(1.5 AS REAL) WHERE root='\(root)'",
            "UPDATE volume_metadata SET entry_count=0 WHERE root='\(root)'",
        ]
        for sql in forbidden {
            await #expect(throws: BrokerError.self, "\(sql)") {
                try await h.connection.write { try h.connection.exec(sql) }
            }
        }
        #expect(await h.store.fetchVolumeLocal(root: root)?.entries == volume("r1").entries)
    }

    /// Corrupt bytes are not served and flag nothing, so a read of a corrupt
    /// leaf never shrinks what a sweep keeps.
    @Test func corruptLeafReadThenSweepRemovesNothingRetained() async throws {
        let h = try harness()
        let top = SerializedVolume(root: cid("top"), entries: [
            cid("top"): Data("top".utf8),
            cid("mid"): Data("mid".utf8),
            cid("top-leaf"): Data("top-leaf".utf8),
        ])
        let mid = volume("mid", ["mid-leaf": Data("mid-leaf".utf8)])
        try await h.store.storeVolumesLocal([top, mid])
        try await h.retained.advanceRetainedRoots(scope: "s", roots: [top.root, mid.root])
        try await h.connection.write {
            try h.connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(cid("top-leaf"))'")
        }
        #expect(await h.store.fetchDataLocal(cid: cid("top-leaf")) == nil)
        #expect(await h.store.fetchVolumeLocal(root: top.root) == nil)

        #expect(try await h.retained.sweep() == 0)
        #expect(await h.store.fetchVolumeLocal(root: mid.root)?.entries == mid.entries)
        #expect(await casRowCount(connection: h.connection, cid: cid("top-leaf")) == 1)

        try await h.store.store(volume: top)
        #expect(await h.store.fetchVolumeLocal(root: top.root)?.entries == top.entries)
    }

    @Test func retainedRootReadPropagatesSQLFailure() async throws {
        let h = try harness()
        #expect(sqlite3_set_authorizer(h.connection.readDb, { _, action, _, _, _, _ in
            action == SQLITE_READ ? SQLITE_DENY : SQLITE_OK
        }, nil) == SQLITE_OK)
        defer { sqlite3_set_authorizer(h.connection.readDb, nil, nil) }

        do {
            _ = try await h.retained.retainedRoots(scope: "canonical")
            Issue.record("retained-root SQL failures must propagate")
        } catch {
            guard let brokerError = error as? BrokerError else {
                Issue.record("unexpected error: \(error)")
                return
            }
            guard case .sqlFailed = brokerError else {
                Issue.record("unexpected broker error: \(brokerError)")
                return
            }
        }
    }

    @Test func sweepSQLFailureRollsBackWholeSweep() async throws {
        let h = try harness()
        let shared = Data("shared".utf8)
        try await h.store.store(volume: volume("drop", ["shared": shared]))
        sqlite3_set_authorizer(h.connection.db, { _, action, table, _, _, _ in
            guard action == SQLITE_DELETE, let table,
                  String(cString: table) == "cas_data" else { return SQLITE_OK }
            return SQLITE_DENY
        }, nil)
        defer { sqlite3_set_authorizer(h.connection.db, nil, nil) }

        await #expect(throws: BrokerError.self) { try await h.retained.sweep() }
        sqlite3_set_authorizer(h.connection.db, nil, nil)
        #expect(await h.store.hasVolume(root: cid("drop")))
        #expect(await casRowCount(connection: h.connection, cid: cid(for: shared)) == 1)
    }

    private func casRowCount(connection: SQLiteConnection, cid: String) async -> Int {
        await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            guard sqlite3_prepare_v2(
                connection.readDb,
                "SELECT COUNT(*) FROM cas_data WHERE cid = ?1",
                -1,
                &stmt,
                nil
            ) == SQLITE_OK, let stmt else { return -1 }
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return -1 }
            return Int(sqlite3_column_int64(stmt, 0))
        }
    }
}
