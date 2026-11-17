import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Named retained-root sets, the release that reclaims named roots, and the
/// sweep that reclaims everything no set names. Replacing a scope with the same set and merging
/// existing roots are naturally idempotent.
struct RetainedRootIndex {
    let connection: SQLiteConnection

    /// Delete every Volume no scope retains and no scope counts (its
    /// membership cascades), with the CAS rows that leaves unowned, in one
    /// transaction on the serial write connection. Every commit leaves each
    /// CAS row owned, so those are all the unowned rows, and the work is in
    /// the Volumes removed and one pass over the Volume list, not in the size of the
    /// content. Advances and stores
    /// serialize against it there, so a root an advance commits first is
    /// kept by the sweep, and an advance that runs after it sees exactly what
    /// survived.
    func sweep() async throws -> Int {
        try await connection.write {
            try connection.transaction {
                try reclaim(roots: textColumn("""
                    SELECT root FROM volume_metadata
                    WHERE root NOT IN (SELECT root FROM retained_roots)
                      AND root NOT IN (SELECT root FROM counted_roots)
                    """))
            }
        }
    }

    func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRoots(roots)
        try Self.validateScope(scope)

        try await connection.write {
            try connection.transaction {
                for root in canonicalRoots {
                    try validateStoredVolume(root: root)
                }

                try connection.execBind("DELETE FROM retained_roots WHERE scope=?1") { stmt in
                    sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                }
                for root in canonicalRoots {
                    try connection.execBind("INSERT INTO retained_roots(scope, root) VALUES(?1, ?2)") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
    }

    func mergeRetainedRoots(scope: String, roots: [String]) async throws {
        let merges = try Self.canonicalMerges([scope: roots])
        try await connection.write {
            try connection.transaction { try merge(merges) }
        }
    }

    /// Each scope with its roots, checked and in order, for `merge`.
    static func canonicalMerges(_ merges: [String: [String]]) throws -> [(scope: String, roots: [String])] {
        try merges.keys.sorted().map { scope in
            let roots = try canonicalRoots(merges[scope] ?? [])
            try validateScope(scope)
            return (scope, roots)
        }
    }

    /// Adds each scope's roots to it, every root a complete stored Volume.
    /// On the write connection, inside the caller's transaction.
    func merge(_ merges: [(scope: String, roots: [String])]) throws {
        for (scope, roots) in merges {
            for root in roots {
                try validateStoredVolume(root: root)
                try connection.execBind("INSERT OR IGNORE INTO retained_roots(scope, root) VALUES(?1, ?2)") { stmt in
                    sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                    sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    /// `roots` leave `scope`; each of them no scope then retains or counts is deleted
    /// (its membership cascades) with the CAS rows it leaves unowned, in one
    /// transaction on the serial write connection. The work is in the roots
    /// named, not in the size of the store.
    func releaseRetainedRoots(scope: String, roots: [String]) async throws -> Int {
        let canonicalRoots = try Self.canonicalRoots(roots)
        try Self.validateScope(scope)

        return try await connection.write {
            try connection.transaction {
                for root in canonicalRoots {
                    try connection.execBind("DELETE FROM retained_roots WHERE scope=?1 AND root=?2") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                return try reclaim(roots: canonicalRoots)
            }
        }
    }

    /// Every root of each of `scopes` leaves it; each of those no scope then
    /// retains or counts is deleted with the CAS rows it leaves unowned, in
    /// one transaction on the serial write connection. The work is in the
    /// roots the scopes retain.
    func releaseRetainedScopes(_ scopes: [String]) async throws -> Int {
        // By their bytes: two spellings Swift calls equal are two scopes.
        let scopes = Dictionary(scopes.map { (Array($0.utf8), $0) }) { first, _ in first }.values.sorted()
        for scope in scopes { try Self.validateScope(scope) }
        guard !scopes.isEmpty else { return 0 }

        return try await connection.write {
            try connection.transaction {
                var released = Set<String>()
                for scope in scopes {
                    released.formUnion(try textColumn("SELECT root FROM retained_roots WHERE scope=?1", scope))
                    try connection.execBind("DELETE FROM retained_roots WHERE scope=?1") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                return try reclaim(roots: released.sorted())
            }
        }
    }

    /// Delete each of `roots` that no scope retains and no scope counts (its
    /// membership cascades), with the CAS rows that leaves unowned. On the
    /// write connection, inside the caller's transaction. Returns the number
    /// of Volumes removed.
    func reclaim(roots: [String]) throws -> Int {
        var removed = 0
        for root in roots {
            let members = try textColumn("SELECT cid FROM volume_entries WHERE root=?1", root)
            try connection.execBind("""
                DELETE FROM volume_metadata
                WHERE root=?1
                  AND NOT EXISTS (SELECT 1 FROM retained_roots WHERE root=?1)
                  AND NOT EXISTS (SELECT 1 FROM counted_roots WHERE root=?1)
                """) { stmt in
                sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            }
            guard sqlite3_changes(connection.db) > 0 else { continue }
            removed += 1
            for cid in members {
                try connection.execBind("""
                    DELETE FROM cas_data
                    WHERE cid=?1 AND NOT EXISTS (SELECT 1 FROM volume_entries WHERE cid=?1)
                    """) { stmt in
                    sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
                }
            }
        }
        return removed
    }

    /// On the write connection, inside the caller's transaction.
    private func textColumn(_ sql: String, _ values: String...) throws -> [String] {
        try connection.withStatement(sql) { stmt in
            guard values.indices.allSatisfy({
                sqlite3_bind_text(stmt, Int32($0 + 1), values[$0], -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK
            }) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
            }
            var result: [String] = []
            while true {
                switch sqlite3_step(stmt) {
                case SQLITE_ROW:
                    guard let ptr = sqlite3_column_text(stmt, 0) else {
                        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
                    }
                    result.append(String(cString: ptr))
                case SQLITE_DONE:
                    return result
                default:
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
                }
            }
        }
    }

    /// One statement, so one snapshot: a seek of the primary key per scope
    /// found, from the first scope not before `prefix` to the first that
    /// does not start with it. The work is in the scopes listed, not in how
    /// many roots they retain. Names compare as bytes, in SQL and in the
    /// in-memory broker alike.
    func retainedScopes(prefix: String) async throws -> [String] {
        // No scope's name holds a NUL, so none starts with such a prefix.
        guard !prefix.utf8.contains(0) else { return [] }
        return try await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let starts = "substr(CAST(scope AS BLOB), 1, length(CAST(?1 AS BLOB))) = CAST(?1 AS BLOB)"
            let sql = """
                WITH RECURSIVE found(scope) AS (
                    SELECT min(scope) FROM retained_roots WHERE scope >= ?1
                    UNION ALL
                    SELECT (SELECT min(scope) FROM retained_roots WHERE scope > found.scope)
                    FROM found WHERE scope IS NOT NULL AND \(starts)
                )
                SELECT scope FROM found WHERE scope IS NOT NULL AND \(starts)
                """
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK,
                  let stmt,
                  sqlite3_bind_text(stmt, 1, prefix, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
            }
            var result: [String] = []
            while true {
                switch sqlite3_step(stmt) {
                case SQLITE_ROW:
                    guard sqlite3_column_type(stmt, 0) == SQLITE_TEXT,
                          let ptr = sqlite3_column_text(stmt, 0) else {
                        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
                    }
                    result.append(String(cString: ptr))
                case SQLITE_DONE:
                    return result
                default:
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
                }
            }
        }
    }

    func retainedRoots(scope: String) async throws -> [String] {
        guard !scope.isEmpty, !scope.utf8.contains(0) else { return [] }
        return try await connection.read {
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = "SELECT root FROM retained_roots WHERE scope=?1 ORDER BY root"
            guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK,
                  let stmt else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
            }
            guard sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
            }
            var result: [String] = []
            while true {
                switch sqlite3_step(stmt) {
                case SQLITE_ROW:
                    guard sqlite3_column_type(stmt, 0) == SQLITE_TEXT,
                          let ptr = sqlite3_column_text(stmt, 0) else {
                        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
                    }
                    result.append(String(cString: ptr))
                case SQLITE_DONE:
                    return result
                default:
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
                }
            }
        }
    }

    static func validateScope(_ scope: String) throws {
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

    static func canonicalRoots(_ roots: [String]) throws -> [String] {
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

    private func validateStoredVolume(root: String) throws {
        guard try CASVolumeStore.isCompleteVolume(root: root, connection: connection) else {
            throw BrokerError.missingRetainedRoot(root)
        }
    }
}
