import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

let SQLITE_TRANSIENT_SHIM = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// Low-level SQLite connection layer.
///
/// Owns the write and read connections, PRAGMA/schema setup, the read/write
/// dispatch queues that serialise SQL access, and the raw `exec`/`execBind`
/// primitives the higher layers build on. Higher layers (CAS store, retained
/// roots) compose this type and never open SQLite handles directly.
final class SQLiteConnection: @unchecked Sendable {
    private static let schemaVersion = 2

    private let readQueue = DispatchQueue(label: "VolumeBroker.DiskBroker.read", qos: .utility, attributes: .concurrent)
    private let writeQueue = DispatchQueue(label: "VolumeBroker.DiskBroker.write", qos: .utility)

    /// Write connection — SQLITE_OPEN_FULLMUTEX serialises all SQL calls.
    let db: OpaquePointer
    /// Separate read-only WAL connection — concurrent reads alongside writes.
    let readDb: OpaquePointer

    init(path: String) throws {
        var handle: OpaquePointer?
        // FULLMUTEX: serialize all SQLite calls on this connection so that
        // the actor's task-to-thread migrations under Swift concurrency cannot
        // race with deinit's sqlite3_close, which runs outside actor isolation.
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        guard sqlite3_open_v2(path, &handle, flags, nil) == SQLITE_OK, let handle else {
            let msg = handle.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown"
            if let handle { sqlite3_close(handle) }
            throw BrokerError.openFailed(msg)
        }
        do {
            try Self.execRaw(db: handle, "PRAGMA busy_timeout=30000")
            try Self.initializeSchemaIfNeeded(db: handle)
            try Self.configureWriteConnection(db: handle)
        } catch {
            sqlite3_close(handle)
            throw error
        }
        self.db = handle

        // Reads have a connection of their own, so that in WAL mode a read is
        // a snapshot of committed state and never waits on a writer. A
        // read-only connection cannot open a WAL database whose `-wal` and
        // `-shm` files do not exist yet, which is every store before its
        // first write and, where SQLite removes them on a clean close, every
        // store reopened: one empty write transaction creates them. There is
        // no falling back to the write connection: a read there would see a
        // transaction half done.
        var rhandle: OpaquePointer?
        do {
            try Self.execRaw(db: handle, "BEGIN IMMEDIATE; COMMIT")
            let rflags = SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX
            guard sqlite3_open_v2(path, &rhandle, rflags, nil) == SQLITE_OK, let opened = rhandle else {
                throw BrokerError.openFailed(
                    rhandle.map { String(cString: sqlite3_errmsg($0)) } ?? "no read connection")
            }
            try Self.enableForeignKeys(db: opened)
            try Self.execRaw(db: opened, "PRAGMA busy_timeout=5000")
            try Self.execRaw(db: opened, "PRAGMA cache_size=-16384")
            self.readDb = opened
        } catch {
            if let rhandle { sqlite3_close(rhandle) }
            sqlite3_close(handle)
            throw error
        }
    }

    deinit {
        sqlite3_close(readDb)
        sqlite3_close(db)
    }

    // MARK: - Serialised access

    /// Run a read on the concurrent read queue against the read-only connection.
    func read<T: Sendable>(_ body: @escaping @Sendable () -> T) async -> T {
        await withCheckedContinuation { continuation in
            readQueue.async {
                continuation.resume(returning: body())
            }
        }
    }

    /// Run a throwing read on the concurrent read queue.
    func read<T: Sendable>(_ body: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            readQueue.async {
                do {
                    continuation.resume(returning: try body())
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    /// Run a write on the serial write queue against the write connection.
    func write<T: Sendable>(_ body: @escaping @Sendable () throws -> T) async throws -> T {
        try await withCheckedThrowingContinuation { continuation in
            writeQueue.async {
                do {
                    continuation.resume(returning: try body())
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    func checkpoint() async {
        _ = try? await write { [self] in
            sqlite3_wal_checkpoint_v2(db, nil, SQLITE_CHECKPOINT_TRUNCATE, nil, nil)
        }
    }

    // MARK: - Primitives (write connection)

    /// Execute a statement-less SQL string on the write connection.
    func exec(_ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    /// Prepare, bind, and step a single write statement to completion.
    func execBind(_ sql: String, bind: (OpaquePointer) -> Void) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        bind(stmt)
        guard sqlite3_step(stmt) == SQLITE_DONE else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    /// Run `body` inside a BEGIN IMMEDIATE / COMMIT transaction on the write
    /// connection, rolling back on any thrown error.
    func transaction<T>(_ body: () throws -> T) throws -> T {
        try exec("BEGIN IMMEDIATE")
        do {
            let result = try body()
            try exec("COMMIT")
            return result
        } catch {
            try? exec("ROLLBACK")
            throw error
        }
    }

    // MARK: - Schema

    static func execRaw(db: OpaquePointer, _ sql: String) throws {
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }

    private static func scalarInt(db: OpaquePointer, _ sql: String) throws -> Int {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt,
              sqlite3_step(stmt) == SQLITE_ROW else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return Int(sqlite3_column_int64(stmt, 0))
    }

    private static func scalarText(db: OpaquePointer, _ sql: String) throws -> String {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt,
              sqlite3_step(stmt) == SQLITE_ROW,
              let text = sqlite3_column_text(stmt, 0) else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return String(cString: text)
    }

    private static func initializeSchemaIfNeeded(db: OpaquePointer) throws {
        try execRaw(db: db, "BEGIN IMMEDIATE")
        do {
            let foundVersion = try scalarInt(db: db, "PRAGMA user_version")
            if foundVersion == schemaVersion {
                try validateSchema(db: db)
            } else {
                guard foundVersion == 0 else {
                    throw BrokerError.migrationRequired(
                        found: foundVersion,
                        required: schemaVersion
                    )
                }
                let objectCount = try scalarInt(
                    db: db,
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
                )
                guard objectCount == 0 else {
                    throw BrokerError.migrationRequired(
                        found: foundVersion,
                        required: schemaVersion
                    )
                }
                try createTables(db: db)
                try execRaw(db: db, "PRAGMA user_version=\(schemaVersion)")
                try validateSchema(db: db)
            }
            try execRaw(db: db, "COMMIT")
        } catch {
            try? execRaw(db: db, "ROLLBACK")
            throw error
        }
    }

    private static let tableSchemas: [(name: String, sql: String)] = [
        ("cas_data", """
            CREATE TABLE cas_data (
                cid TEXT PRIMARY KEY,
                data BLOB NOT NULL
            )
            """),
        ("volume_metadata", """
            CREATE TABLE volume_metadata (
                root TEXT PRIMARY KEY,
                entry_count INTEGER NOT NULL
                    CHECK (typeof(entry_count) = 'integer' AND entry_count > 0)
            )
            """),
        ("volume_entries", """
            CREATE TABLE volume_entries (
                root TEXT NOT NULL REFERENCES volume_metadata(root) ON DELETE CASCADE,
                cid TEXT NOT NULL REFERENCES cas_data(cid),
                PRIMARY KEY (root, cid)
            )
            """),
        ("retained_roots", """
            CREATE TABLE retained_roots (
                scope TEXT NOT NULL,
                root TEXT NOT NULL,
                PRIMARY KEY (scope, root)
            )
            """),
    ]

    private static let indexSchemas: [(name: String, sql: String)] = [
        ("idx_ve_cid", "CREATE INDEX idx_ve_cid ON volume_entries(cid)"),
        ("idx_ve_root", "CREATE INDEX idx_ve_root ON volume_entries(root)"),
        ("idx_retained_roots_root", "CREATE INDEX idx_retained_roots_root ON retained_roots(root)"),
    ]

    private static func validateSchema(db: OpaquePointer) throws {
        for table in tableSchemas {
            guard let stored = schemaSQL(db: db, type: "table", name: table.name),
                  canonicalSQL(stored) == canonicalSQL(table.sql) else {
                throw BrokerError.invalidSchema(version: schemaVersion)
            }
        }
        for index in indexSchemas {
            guard let stored = schemaSQL(db: db, type: "index", name: index.name),
                  canonicalSQL(stored) == canonicalSQL(index.sql) else {
                throw BrokerError.invalidSchema(version: schemaVersion)
            }
        }

        let protectedTables = tableSchemas.map { "'\($0.name)'" }.joined(separator: ",")
        let allowedIndexes = indexSchemas.map { "'\($0.name)'" }.joined(separator: ",")
        guard try scalarInt(
            db: db,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND tbl_name IN (\(protectedTables))"
        ) == 0,
        try scalarInt(
            db: db,
            """
            SELECT COUNT(*) FROM sqlite_schema
            WHERE type='index'
              AND tbl_name IN (\(protectedTables))
              AND sql IS NOT NULL
              AND name NOT IN (\(allowedIndexes))
            """
        ) == 0,
        try scalarInt(
            db: db,
            """
            SELECT COUNT(*)
            FROM sqlite_schema AS child
            JOIN pragma_foreign_key_list(child.name) AS fk
            WHERE child.type='table'
              AND child.name NOT IN (\(protectedTables))
              AND fk."table" COLLATE NOCASE IN (\(protectedTables))
            """
        ) == 0,
        try scalarInt(db: db, "SELECT COUNT(*) FROM pragma_foreign_key_check") == 0 else {
            throw BrokerError.invalidSchema(version: schemaVersion)
        }
    }

    private static func schemaSQL(db: OpaquePointer, type: String, name: String) -> String? {
        var statement: OpaquePointer?
        defer { sqlite3_finalize(statement) }
        guard sqlite3_prepare_v2(
            db,
            "SELECT sql FROM sqlite_schema WHERE type=?1 AND name=?2",
            -1,
            &statement,
            nil
        ) == SQLITE_OK, let statement else { return nil }
        sqlite3_bind_text(statement, 1, type, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_text(statement, 2, name, -1, SQLITE_TRANSIENT_SHIM)
        guard sqlite3_step(statement) == SQLITE_ROW,
              let sql = sqlite3_column_text(statement, 0) else { return nil }
        return String(cString: sql)
    }

    private static func canonicalSQL(_ sql: String) -> String {
        func needsSeparator(_ lhs: Character, _ rhs: Character) -> Bool {
            let quotes: Set<Character> = ["'", "\"", "`"]
            let lhsIsWord = lhs.isLetter || lhs.isNumber || lhs == "_" || lhs == "$"
            let rhsIsWord = rhs.isLetter || rhs.isNumber || rhs == "_" || rhs == "$"
            return (lhsIsWord && rhsIsWord) || quotes.contains(lhs) || quotes.contains(rhs)
        }

        var normalized = ""
        var quote: Character?
        var pendingSpace = false
        let characters = Array(sql.trimmingCharacters(in: .whitespacesAndNewlines))
        var index = 0

        while index < characters.count {
            let character = characters[index]
            if let activeQuote = quote {
                normalized.append(character)
                if character == activeQuote {
                    if index + 1 < characters.count, characters[index + 1] == activeQuote {
                        index += 1
                        normalized.append(characters[index])
                    } else {
                        quote = nil
                    }
                }
            } else if character == "'" || character == "\"" || character == "`" {
                if pendingSpace, let previous = normalized.last,
                   needsSeparator(previous, character) { normalized.append(" ") }
                pendingSpace = false
                quote = character
                normalized.append(character)
            } else if character.isWhitespace {
                pendingSpace = true
            } else {
                if pendingSpace, let previous = normalized.last,
                   needsSeparator(previous, character) { normalized.append(" ") }
                pendingSpace = false
                normalized.append(character)
            }
            index += 1
        }
        return normalized
    }

    private static func configureWriteConnection(db: OpaquePointer) throws {
        try enableForeignKeys(db: db)
        guard try scalarText(db: db, "PRAGMA journal_mode=WAL").lowercased() == "wal" else {
            throw BrokerError.sqlFailed("SQLite journal_mode=WAL unavailable")
        }
        // Reads and writes wait out short WAL/checkpoint contention windows.
        try execRaw(db: db, "PRAGMA busy_timeout=5000")
        try execRaw(db: db, "PRAGMA synchronous=FULL")
        guard try scalarInt(db: db, "PRAGMA synchronous") == 2 else {
            throw BrokerError.sqlFailed("SQLite synchronous=FULL unavailable")
        }
        try execRaw(db: db, "PRAGMA cache_size=-65536")
        try execRaw(db: db, "PRAGMA mmap_size=268435456")
        try execRaw(db: db, "PRAGMA temp_store=MEMORY")
    }

    private static func enableForeignKeys(db: OpaquePointer) throws {
        try execRaw(db: db, "PRAGMA foreign_keys=ON")
        guard try scalarInt(db: db, "PRAGMA foreign_keys") == 1 else {
            throw BrokerError.sqlFailed("foreign key enforcement unavailable")
        }
    }

    private static func createTables(db: OpaquePointer) throws {
        for table in tableSchemas { try execRaw(db: db, table.sql) }
        for index in indexSchemas { try execRaw(db: db, index.sql) }
    }
}
