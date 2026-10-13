import Foundation
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Named retained-root sets and the sweep that reclaims everything they do
/// not name. Replacing a scope with the same set and merging
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
        return unique.sorted()
    }

    private func validateStoredVolume(root: String) throws {
        guard try CASVolumeStore.isCompleteVolume(root: root, db: connection.db) else {
            throw BrokerError.missingRetainedRoot(root)
        }
    }
}
