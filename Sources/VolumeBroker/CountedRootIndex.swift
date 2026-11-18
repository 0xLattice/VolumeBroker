import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Counted roots: per scope, a count for each root and one marker, changed
/// together under a compare-and-swap of the marker. A counted root keeps its
/// Volume as a retained root does; every reclaim checks both.
struct CountedRootIndex {
    let connection: SQLiteConnection
    let retained: RetainedRootIndex

    func adjustCountedRoots(
        scope: String,
        deltas: [String: Int],
        marker: String,
        expectedMarker: String?
    ) async throws {
        try RetainedRootIndex.validateScope(scope)
        try Self.validateMarker(marker)
        if let expectedMarker { try Self.validateMarker(expectedMarker) }
        let roots = try RetainedRootIndex.canonicalRoots(Array(deltas.keys))
        let changes = roots.compactMap { root in deltas[root].flatMap { $0 == 0 ? nil : (root, $0) } }

        try await connection.write {
            try connection.transaction {
                let found = try storedMarker(scope: scope)
                guard found.map({ Array($0.utf8) }) == expectedMarker.map({ Array($0.utf8) }) else {
                    throw BrokerError.countedMarkerMismatch(expected: expectedMarker, found: found)
                }

                var updated: [(root: String, count: Int)] = []
                var missing: [String] = []
                for (root, delta) in changes {
                    let current = try storedCount(scope: scope, root: root)
                    let (count, overflow) = current.addingReportingOverflow(delta)
                    guard !overflow else { throw BrokerError.countedRootOverflow(root) }
                    guard count >= 0 else { throw BrokerError.negativeCount(root) }
                    if current == 0, count > 0,
                       try !CASVolumeStore.isCompleteVolume(root: root, connection: connection) {
                        missing.append(root)
                    }
                    updated.append((root, count))
                }
                guard missing.isEmpty else { throw BrokerError.countedRootsMissing(missing) }

                var zeroed: [String] = []
                for (root, count) in updated {
                    if count == 0 {
                        try connection.execBind("DELETE FROM counted_roots WHERE scope=?1 AND root=?2") { stmt in
                            sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                            sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                        }
                        zeroed.append(root)
                    } else {
                        try connection.execBind("""
                            INSERT INTO counted_roots(scope, root, count) VALUES(?1, ?2, ?3)
                            ON CONFLICT(scope, root) DO UPDATE SET count=excluded.count
                            """) { stmt in
                            sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                            sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                            sqlite3_bind_int64(stmt, 3, Int64(count))
                        }
                    }
                }
                try connection.execBind("""
                    INSERT INTO counted_markers(scope, marker) VALUES(?1, ?2)
                    ON CONFLICT(scope) DO UPDATE SET marker=excluded.marker
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, marker, -1, SQLITE_TRANSIENT_SHIM)
                }
                _ = try retained.reclaim(roots: zeroed)
            }
        }
    }

    func dropCountedScope(scope: String) async throws {
        try RetainedRootIndex.validateScope(scope)
        try await connection.write {
            try connection.transaction {
                let roots = try connection.withStatement(
                    "SELECT root FROM counted_roots WHERE scope=?1 ORDER BY root"
                ) { stmt in
                    try textColumn(stmt: stmt, db: connection.db, scope)
                }
                for sql in ["DELETE FROM counted_roots WHERE scope=?1", "DELETE FROM counted_markers WHERE scope=?1"] {
                    try connection.execBind(sql) { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                _ = try retained.reclaim(roots: roots)
            }
        }
    }

    /// One statement, so one snapshot: the marker and the counts it was
    /// written with.
    func countedState(scope: String) async throws -> (marker: String?, counts: [String: Int]) {
        guard !scope.isEmpty, !scope.utf8.contains(0) else { return (nil, [:]) }
        return try await connection.read { readDb in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let db = readDb
            let sql = """
                SELECT NULL, marker FROM counted_markers WHERE scope=?1
                UNION ALL
                SELECT count, root FROM counted_roots WHERE scope=?1
                """
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
                  let stmt,
                  sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
            var marker: String?
            var counts: [String: Int] = [:]
            while true {
                switch sqlite3_step(stmt) {
                case SQLITE_ROW:
                    guard sqlite3_column_type(stmt, 1) == SQLITE_TEXT,
                          let ptr = sqlite3_column_text(stmt, 1) else {
                        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
                    }
                    switch sqlite3_column_type(stmt, 0) {
                    case SQLITE_NULL:
                        marker = String(cString: ptr)
                    case SQLITE_INTEGER:
                        counts[String(cString: ptr)] = Int(sqlite3_column_int64(stmt, 0))
                    default:
                        throw BrokerError.sqlFailed("read counted root")
                    }
                case SQLITE_DONE:
                    return (marker, counts)
                default:
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
                }
            }
        }
    }

    /// One row per scope, so a range of the primary key from `prefix`; the
    /// work is in the scopes from `prefix` on. Names compare as bytes.
    func countedScopes(prefix: String) async throws -> [String] {
        guard !prefix.utf8.contains(0) else { return [] }
        return try await connection.read { readDb in
            try textColumn(
                db: readDb,
                """
                SELECT scope FROM counted_markers
                WHERE scope >= ?1
                  AND substr(CAST(scope AS BLOB), 1, length(CAST(?1 AS BLOB))) = CAST(?1 AS BLOB)
                ORDER BY scope
                """,
                prefix
            )
        }
    }

    // MARK: - Helpers

    /// On the write connection, inside the caller's transaction.
    private func storedMarker(scope: String) throws -> String? {
        try connection.withStatement("SELECT marker FROM counted_markers WHERE scope=?1") { stmt in
            try textColumn(stmt: stmt, db: connection.db, scope)
        }.first
    }

    /// On the write connection, inside the caller's transaction. Zero when
    /// the scope does not count the root.
    private func storedCount(scope: String, root: String) throws -> Int {
        let db = connection.db
        return try connection.withStatement("SELECT count FROM counted_roots WHERE scope=?1 AND root=?2") { stmt in
            sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
            switch sqlite3_step(stmt) {
            case SQLITE_ROW: return Int(sqlite3_column_int64(stmt, 0))
            case SQLITE_DONE: return 0
            default: throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
        }
    }

    /// The first column of every row `sql` yields with `?1` bound to
    /// `value`. On the read connection: each read prepares its own.
    private func textColumn(db: OpaquePointer, _ sql: String, _ value: String) throws -> [String] {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return try textColumn(stmt: stmt, db: db, value)
    }

    /// The first column of every row `stmt` yields with `?1` bound to `value`.
    private func textColumn(stmt: OpaquePointer, db: OpaquePointer, _ value: String) throws -> [String] {
        guard sqlite3_bind_text(stmt, 1, value, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        var result: [String] = []
        while true {
            switch sqlite3_step(stmt) {
            case SQLITE_ROW:
                guard sqlite3_column_type(stmt, 0) == SQLITE_TEXT,
                      let ptr = sqlite3_column_text(stmt, 0) else {
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
                }
                result.append(String(cString: ptr))
            case SQLITE_DONE:
                return result
            default:
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
        }
    }

    /// A marker is named as a scope is: SQLite reads a bound string up to its
    /// first NUL, so such a marker would stand for another.
    static func validateMarker(_ marker: String) throws {
        guard !marker.isEmpty else {
            throw BrokerError.invalidCountedMarker("marker must not be empty")
        }
        guard !marker.utf8.contains(0) else {
            throw BrokerError.invalidCountedMarker("marker must not contain NUL")
        }
    }
}
