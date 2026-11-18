import CID
import Foundation
import Multihash
import Testing
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif
@testable import VolumeBroker

@Suite("Schema version")
struct SchemaVersionTests {
    private enum TestDatabaseError: Error {
        case sqlite(String)
    }

    private func temporaryDatabase() throws -> (directory: URL, path: String) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerSchema-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return (directory, directory.appendingPathComponent("volumes.sqlite").path)
    }

    private func withDatabase<T>(at path: String, _ body: (OpaquePointer) throws -> T) throws -> T {
        var db: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        guard sqlite3_open_v2(path, &db, flags, nil) == SQLITE_OK, let db else {
            let message = db.map { String(cString: sqlite3_errmsg($0)) } ?? "open failed"
            if let db { sqlite3_close(db) }
            throw TestDatabaseError.sqlite(message)
        }
        defer { sqlite3_close(db) }
        sqlite3_busy_timeout(db, 5_000)
        return try body(db)
    }

    private func execute(_ db: OpaquePointer, _ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw TestDatabaseError.sqlite(String(cString: sqlite3_errmsg(db)))
        }
    }

    private func scalar(_ db: OpaquePointer, _ sql: String) throws -> Int {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt,
              sqlite3_step(stmt) == SQLITE_ROW else {
            throw TestDatabaseError.sqlite(String(cString: sqlite3_errmsg(db)))
        }
        return Int(sqlite3_column_int64(stmt, 0))
    }

    private func scalarText(_ db: OpaquePointer, _ sql: String) throws -> String {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt,
              sqlite3_step(stmt) == SQLITE_ROW,
              let value = sqlite3_column_text(stmt, 0) else {
            throw TestDatabaseError.sqlite(String(cString: sqlite3_errmsg(db)))
        }
        return String(cString: value)
    }

    private func cid(for data: Data) throws -> String {
        let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
        return try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    @Test func freshDatabaseInitializesV2AndReopens() async throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let data = Data("root".utf8)
        let root = try cid(for: data)

        let broker = try DiskBroker(path: location.path)
        try await broker.store(volume: SerializedVolume(root: root, entries: [root: data]))
        #expect(await broker.hasVolume(root: root))

        try withDatabase(at: location.path) { db in
            let version = try scalar(db, "PRAGMA user_version")
            #expect(version == 2)
        }

        let reopened = try DiskBroker(path: location.path)
        #expect(await reopened.fetchVolumeLocal(root: root)?.entries == [root: data])
        #expect(await broker.hasVolume(root: root))
    }

    @Test func equivalentWhitespaceV2MetadataSchemaReopens() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            try execute(db, "DROP TABLE volume_metadata")
            try execute(db, """
                CREATE TABLE volume_metadata
                (root TEXT PRIMARY KEY, entry_count INTEGER NOT NULL
                CHECK (typeof(entry_count) = 'integer'   AND entry_count > 0))
                """)
        }

        _ = try DiskBroker(path: location.path)
    }

    @Test func foreignKeysAreEnabledOnBothConnections() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let connection = try SQLiteConnection(path: location.path)

        let writeForeignKeys = try scalar(connection.db, "PRAGMA foreign_keys")
        let readForeignKeys = try connection.readers.map { try scalar($0, "PRAGMA foreign_keys") }
        #expect(writeForeignKeys == 1)
        #expect(readForeignKeys == Array(repeating: 1, count: SQLiteConnection.readerCount))
    }

    @Test func writeConnectionUsesWALAndFullSynchronous() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let connection = try SQLiteConnection(path: location.path)

        #expect(try scalarText(connection.db, "PRAGMA journal_mode").lowercased() == "wal")
        #expect(try scalar(connection.db, "PRAGMA synchronous") == 2)
    }

    @Test func freshSchemaOmitsOperationJournals() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        _ = try DiskBroker(path: location.path)

        try withDatabase(at: location.path) { db in
            let operationTables = try scalar(db, """
                SELECT COUNT(*) FROM sqlite_schema
                WHERE type='table'
                  AND name IN ('volume_unpin_operations', 'retained_root_operations')
                """)
            #expect(operationTables == 0)
        }
    }

    @Test func legacyOperationTablesReopenUntouched() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            try execute(db, """
                CREATE TABLE volume_unpin_operations (
                    operation_id TEXT PRIMARY KEY
                )
                """)
            try execute(db, """
                CREATE TABLE retained_root_operations (
                    operation_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    canonical_roots TEXT NOT NULL
                )
                """)
            try execute(db, "INSERT INTO volume_unpin_operations VALUES('legacy-unpin')")
            try execute(db, "INSERT INTO retained_root_operations VALUES('legacy-retain', 'scope', '[]')")
        }

        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            let unpinRows = try scalar(
                db,
                "SELECT COUNT(*) FROM volume_unpin_operations WHERE operation_id='legacy-unpin'"
            )
            let retainedRows = try scalar(
                db,
                "SELECT COUNT(*) FROM retained_root_operations WHERE operation_id='legacy-retain'"
            )
            #expect(unpinRows == 1)
            #expect(retainedRows == 1)
        }
    }

    @Test func concurrentFreshOpenInitializesOnce() async throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }

        let brokers = try await withThrowingTaskGroup(
            of: DiskBroker.self,
            returning: [DiskBroker].self
        ) { group in
            for _ in 0..<16 {
                group.addTask { try DiskBroker(path: location.path) }
            }
            var result: [DiskBroker] = []
            for try await broker in group { result.append(broker) }
            return result
        }

        #expect(brokers.count == 16)
        try withDatabase(at: location.path) { db in
            let version = try scalar(db, "PRAGMA user_version")
            #expect(version == 2)
        }
    }

    @Test func legacyV0StoreIsRejectedWithoutMutation() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        try withDatabase(at: location.path) { db in
            try execute(db, "CREATE TABLE cas_data (cid TEXT PRIMARY KEY, data BLOB NOT NULL)")
            try execute(db, """
                CREATE TABLE volume_entries (
                    root TEXT NOT NULL,
                    cid TEXT NOT NULL,
                    PRIMARY KEY (root, cid)
                )
                """)
            try execute(db, """
                CREATE TABLE volume_metadata (
                    root TEXT PRIMARY KEY,
                    stored_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """)
            try execute(db, "INSERT INTO cas_data(cid, data) VALUES('legacy-root', X'CAFE')")
            try execute(db, "INSERT INTO volume_entries(root, cid) VALUES('legacy-root', 'legacy-root')")
            try execute(db, "INSERT INTO volume_metadata(root) VALUES('legacy-root')")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("nonempty v0 database must require migration")
        } catch {
            #expect(error as? BrokerError == .migrationRequired(found: 0, required: 2))
        }

        try withDatabase(at: location.path) { db in
            let version = try scalar(db, "PRAGMA user_version")
            let contentRows = try scalar(
                db,
                "SELECT COUNT(*) FROM cas_data WHERE cid='legacy-root'"
            )
            let metadataColumns = try scalar(
                db,
                "SELECT COUNT(*) FROM pragma_table_info('volume_metadata')"
            )
            #expect(version == 0)
            #expect(contentRows == 1)
            #expect(metadataColumns == 2)
        }
    }

    /// v1 carried owner/count pins and pin expiry. It is not migrated: the
    /// store is rebuilt from a clean path.
    @Test func v1StoreIsRejectedWithoutMutation() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        try withDatabase(at: location.path) { db in
            try execute(db, "CREATE TABLE volume_pins (root TEXT NOT NULL, owner TEXT NOT NULL)")
            try execute(db, "INSERT INTO volume_pins VALUES('root', 'owner')")
            try execute(db, "PRAGMA user_version=1")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("v1 schema must fail closed")
        } catch {
            #expect(error as? BrokerError == .migrationRequired(found: 1, required: 2))
        }
        try withDatabase(at: location.path) { db in
            let version = try scalar(db, "PRAGMA user_version")
            let pinRows = try scalar(db, "SELECT COUNT(*) FROM volume_pins")
            #expect(version == 1)
            #expect(pinRows == 1)
        }
    }

    @Test func futureVersionIsRejectedWithoutMutation() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        try withDatabase(at: location.path) { db in
            try execute(db, "PRAGMA user_version=3")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("future schema must fail closed")
        } catch {
            #expect(error as? BrokerError == .migrationRequired(found: 3, required: 2))
        }
        try withDatabase(at: location.path) { db in
            let version = try scalar(db, "PRAGMA user_version")
            let brokerTables = try scalar(db, "SELECT COUNT(*) FROM sqlite_schema WHERE name='cas_data'")
            #expect(version == 3)
            #expect(brokerTables == 0)
        }
    }

    @Test func stampedV2WithoutV2SchemaIsRejected() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        try withDatabase(at: location.path) { db in
            try execute(db, "PRAGMA user_version=2")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("schema stamp without schema must fail closed")
        } catch {
            #expect(error as? BrokerError == .invalidSchema(version: 2))
        }
    }

    @Test func behaviorChangingV2SchemaIsRejected() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        do { _ = try DiskBroker(path: location.path) }
        try withDatabase(at: location.path) { db in
            try execute(db, "DROP TABLE volume_metadata")
            try execute(db, """
                CREATE TABLE volume_metadata (
                    root TEXT PRIMARY KEY,
                    entry_count INTEGER NOT NULL CHECK (entry_count > 0)
                )
                """)
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("behavior-changing v2 schema must fail closed")
        } catch {
            #expect(error as? BrokerError == .invalidSchema(version: 2))
        }
    }

    @Test func missingRequiredIndexIsRejected() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        do { _ = try DiskBroker(path: location.path) }
        try withDatabase(at: location.path) { db in
            try execute(db, "DROP INDEX idx_ve_cid")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("missing canonical index must fail closed")
        } catch {
            #expect(error as? BrokerError == .invalidSchema(version: 2))
        }
    }

    @Test func unexpectedIndexOnOwnedTableIsRejected() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        do { _ = try DiskBroker(path: location.path) }
        try withDatabase(at: location.path) { db in
            try execute(db, "CREATE UNIQUE INDEX bad_owner ON retained_roots(root)")
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("behavior-changing index must fail closed")
        } catch {
            #expect(error as? BrokerError == .invalidSchema(version: 2))
        }
    }

    @Test func reopeningV2PreservesExistingTables() throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            try execute(db, "CREATE TABLE volume_edges(marker TEXT NOT NULL)")
            try execute(db, "INSERT INTO volume_edges(marker) VALUES('keep')")
        }

        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            let preservedRows = try scalar(db, "SELECT COUNT(*) FROM volume_edges WHERE marker='keep'")
            #expect(preservedRows == 1)
        }
    }

    @Test(arguments: ["CASCADE", "RESTRICT"])
    func incomingForeignKeyToOwnedTableIsRejected(action: String) throws {
        let location = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        _ = try DiskBroker(path: location.path)
        try withDatabase(at: location.path) { db in
            try execute(db, """
                CREATE TABLE external_volume_reference (
                    root TEXT REFERENCES volume_metadata(root) ON DELETE \(action)
                )
                """)
        }

        do {
            _ = try DiskBroker(path: location.path)
            Issue.record("incoming foreign keys must not attach behavior to owned tables")
        } catch {
            #expect(error as? BrokerError == .invalidSchema(version: 2))
        }
    }
}
