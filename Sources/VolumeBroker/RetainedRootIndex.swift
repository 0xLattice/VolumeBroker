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

    /// Delete every Volume no scope retains (its membership cascades), then every
    /// CAS row no surviving Volume owns, in one transaction on the serial
    /// write connection. Advances and stores serialize against it there, so a
    /// root an advance commits first is kept by the sweep, and an advance
    /// that runs after it sees exactly what survived.
    func sweep() async throws -> Int {
        try await connection.write {
            try connection.transaction {
                try connection.exec("""
                    DELETE FROM volume_metadata
                    WHERE root NOT IN (SELECT root FROM retained_roots)
                    """)
                let removed = Int(sqlite3_changes(connection.db))
                try connection.exec("""
                    DELETE FROM cas_data
                    WHERE NOT EXISTS (
                        SELECT 1 FROM volume_entries ve WHERE ve.cid = cas_data.cid
                    )
                    """)
                return removed
            }
        }
    }

    func advanceRetainedRoots(scope: String, roots: [String]) async throws {
        let canonicalRoots = try Self.canonicalRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }

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
        let canonicalRoots = try Self.canonicalRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }

        try await connection.write {
            try connection.transaction {
                for root in canonicalRoots {
                    try validateStoredVolume(root: root)
                    try connection.execBind("INSERT OR IGNORE INTO retained_roots(scope, root) VALUES(?1, ?2)") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
            }
        }
    }

    /// `roots` leave `scope`; each of them no scope then retains is deleted
    /// (its membership cascades) with the CAS rows it leaves unowned, in one
    /// transaction on the serial write connection. The work is in the roots
    /// named, not in the size of the store.
    func releaseRetainedRoots(scope: String, roots: [String]) async throws -> Int {
        let canonicalRoots = try Self.canonicalRoots(roots)
        guard !scope.isEmpty else {
            throw BrokerError.invalidRetainedRoots("scope must not be empty")
        }

        return try await connection.write {
            try connection.transaction {
                for root in canonicalRoots {
                    try connection.execBind("DELETE FROM retained_roots WHERE scope=?1 AND root=?2") { stmt in
                        sqlite3_bind_text(stmt, 1, scope, -1, SQLITE_TRANSIENT_SHIM)
                        sqlite3_bind_text(stmt, 2, root, -1, SQLITE_TRANSIENT_SHIM)
                    }
                }
                var removed = 0
                for root in canonicalRoots {
                    let members = try memberCIDs(root: root)
                    try connection.execBind("""
                        DELETE FROM volume_metadata
                        WHERE root=?1 AND NOT EXISTS (SELECT 1 FROM retained_roots WHERE root=?1)
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
        }
    }

    /// On the write connection, inside the caller's transaction.
    private func memberCIDs(root: String) throws -> [String] {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(connection.db, "SELECT cid FROM volume_entries WHERE root=?1", -1, &stmt, nil) == SQLITE_OK,
              let stmt,
              sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
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

    /// One seek of the primary key per scope found, from the first scope
    /// not before `prefix` to the first that does not start with it: the
    /// work is in the scopes listed, not in how many roots they retain.
    /// Scopes compare as bytes, as the in-memory broker compares them.
    func retainedScopes(prefix: String) async throws -> [String] {
        try await connection.read {
            var result: [String] = []
            var next = try scope(notBefore: prefix, strictly: false)
            while let scope = next, scope.utf8.starts(with: prefix.utf8) {
                result.append(scope)
                next = try self.scope(notBefore: scope, strictly: true)
            }
            return result
        }
    }

    /// On the read connection.
    private func scope(notBefore bound: String, strictly: Bool) throws -> String? {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = "SELECT scope FROM retained_roots WHERE scope \(strictly ? ">" : ">=") ?1 ORDER BY scope LIMIT 1"
        guard sqlite3_prepare_v2(connection.readDb, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt,
              sqlite3_bind_text(stmt, 1, bound, -1, SQLITE_TRANSIENT_SHIM) == SQLITE_OK else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
        }
        switch sqlite3_step(stmt) {
        case SQLITE_ROW:
            guard let ptr = sqlite3_column_text(stmt, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
            }
            return String(cString: ptr)
        case SQLITE_DONE:
            return nil
        default:
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.readDb)))
        }
    }

    func retainedRoots(scope: String) async throws -> [String] {
        guard !scope.isEmpty else { return [] }
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

    private static func canonicalRoots(_ roots: [String]) throws -> [String] {
        let unique = Array(Set(roots))
        if unique.contains(where: { $0.isEmpty }) {
            throw BrokerError.invalidRetainedRoots("roots must not contain empty strings")
        }
        // SQLite reads a bound string up to its first NUL, so such a name
        // would stand for another.
        if unique.contains(where: { $0.contains("\0") }) {
            throw BrokerError.invalidRetainedRoots("roots must not contain NUL")
        }
        return unique.sorted()
    }

    private func validateStoredVolume(root: String) throws {
        guard try CASVolumeStore.isCompleteVolume(root: root, db: connection.db) else {
            throw BrokerError.missingRetainedRoot(root)
        }
    }
}
