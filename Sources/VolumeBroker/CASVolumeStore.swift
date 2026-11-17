import Foundation
import cashew
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif

/// Content-addressed storage for complete published Volumes.
struct CASVolumeStore: VolumeStorer {
    private static let maximumReadParameters = 500
    let connection: SQLiteConnection

    /// Every durable read uses this predicate. Foreign keys guarantee each
    /// member has a CAS row and each member row has metadata, and the schema
    /// guarantees an integer, positive `entry_count`. What remains is that the
    /// declared count matches membership and the root is one of its members.
    static let completeVolumePredicate = """
        vm.entry_count = (
            SELECT COUNT(*) FROM volume_entries manifest_members
            WHERE manifest_members.root = vm.root
        )
        AND EXISTS (
            SELECT 1 FROM volume_entries root_member
            WHERE root_member.root = vm.root AND root_member.cid = vm.root
        )
        """

    func hasVolume(root: String) async -> Bool {
        await connection.read {
            (try? Self.isCompleteVolume(root: root, db: connection.readDb)) ?? false
        }
    }

    private static let isCompleteVolumeSQL = """
        SELECT 1
        FROM volume_metadata vm
        WHERE vm.root = ?1
          AND \(completeVolumePredicate)
        LIMIT 1
        """

    /// On the read connection: each read prepares its own.
    static func isCompleteVolume(root: String, db: OpaquePointer) throws -> Bool {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(db, isCompleteVolumeSQL, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        return try isCompleteVolume(root: root, stmt: stmt, db: db)
    }

    /// On the write connection, inside the caller's transaction.
    static func isCompleteVolume(root: String, connection: SQLiteConnection) throws -> Bool {
        try connection.withStatement(isCompleteVolumeSQL) { stmt in
            try isCompleteVolume(root: root, stmt: stmt, db: connection.db)
        }
    }

    private static func isCompleteVolume(root: String, stmt: OpaquePointer, db: OpaquePointer) throws -> Bool {
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        let result = sqlite3_step(stmt)
        if result == SQLITE_ROW { return true }
        if result == SQLITE_DONE { return false }
        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
    }

    /// The bytes are served as stored, not hashed again: they were checked
    /// on their way in. Bytes damaged since are served as they are, and a
    /// re-store of the right bytes overwrites them.
    func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await connection.read {
            (try? Self.loadVolume(root: root, db: connection.readDb)) ?? nil
        }
    }

    static func loadVolume(root: String, db: OpaquePointer) throws -> SerializedVolume? {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT ve.cid, cd.data
            FROM volume_metadata vm
            JOIN volume_entries ve ON ve.root = vm.root
            JOIN cas_data cd ON cd.cid = ve.cid
            WHERE vm.root = ?1
              AND \(Self.completeVolumePredicate)
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)

        var entries: [String: Data] = [:]
        while true {
            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { break }
            guard result == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
            guard let cidPtr = sqlite3_column_text(stmt, 0) else {
                throw BrokerError.sqlFailed("read Volume member CID")
            }
            let cid = String(cString: cidPtr)
            guard sqlite3_column_type(stmt, 1) != SQLITE_NULL else { return nil }
            let length = Int(sqlite3_column_bytes(stmt, 1))
            if length == 0 {
                entries[cid] = Data()
            } else if let bytes = sqlite3_column_blob(stmt, 1) {
                entries[cid] = Data(bytes: bytes, count: length)
            } else {
                return nil
            }
        }
        guard !entries.isEmpty else { return nil }
        return SerializedVolume(root: root, entries: entries)
    }

    /// A CAS row is readable only through at least one complete owning Volume.
    func fetchDataLocal(cid: String) async -> Data? {
        await fetchDataLocal(cids: [cid])[cid]
    }

    /// A batched point read preserves the same serve gate as scalar reads
    /// while using bounded SQLite parameter sets.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        guard !cids.isEmpty else { return [:] }
        let loaded: [String: Data]? = await connection.read {
            try? Self.loadServableData(cids: cids, db: connection.readDb)
        }
        return loaded ?? [:]
    }

    private static func loadServableData(
        cids: Set<String>,
        db: OpaquePointer
    ) throws -> [String: Data] {
        var found: [String: Data] = [:]
        found.reserveCapacity(cids.count)
        let ordered = Array(cids)
        for start in stride(from: 0, to: ordered.count, by: maximumReadParameters) {
            let chunk = ordered[start..<min(start + maximumReadParameters, ordered.count)]
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let placeholders = (1...chunk.count).map { "?\($0)" }.joined(separator: ",")
            let sql = """
                SELECT cd.cid, cd.data
                FROM cas_data cd
                WHERE cd.cid IN (\(placeholders))
                  AND EXISTS (
                      SELECT 1
                      FROM volume_entries owner
                      JOIN volume_metadata vm ON vm.root = owner.root
                      WHERE owner.cid = cd.cid
                        AND \(Self.completeVolumePredicate)
                  )
                """
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
                  let stmt else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
            for (offset, cid) in chunk.enumerated() {
                sqlite3_bind_text(stmt, Int32(offset + 1), cid, -1, SQLITE_TRANSIENT_SHIM)
            }
            while true {
                let result = sqlite3_step(stmt)
                if result == SQLITE_DONE { break }
                guard result == SQLITE_ROW,
                      let cid = sqlite3_column_text(stmt, 0),
                      sqlite3_column_type(stmt, 1) != SQLITE_NULL else {
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
                }
                let key = String(cString: cid)
                let length = Int(sqlite3_column_bytes(stmt, 1))
                if length == 0 {
                    found[key] = Data()
                } else if let bytes = sqlite3_column_blob(stmt, 1) {
                    found[key] = Data(bytes: bytes, count: length)
                } else {
                    throw BrokerError.sqlFailed("read CAS content")
                }
            }
        }
        return found
    }

    func store(volume: SerializedVolume) async throws {
        try await storeVolumesLocal([volume])
    }

    func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
        try await storeVolumesLocal(volumes.map(ValidatedVolume.init(validating:)), then: nil)
    }

    /// Stores `volumes`, then runs `then` on the write connection, in one
    /// transaction: a throw from either changes nothing.
    func storeVolumesLocal(
        _ volumes: [ValidatedVolume], then: (@Sendable () throws -> Void)?
    ) async throws {
        let batch = try Self.validateBatch(volumes)
        guard !batch.volumes.isEmpty || then != nil else { return }

        try await connection.write {
            try connection.transaction {
                for volume in batch.volumes {
                    try validateExistingMembership(of: volume)
                }
                for (cid, data) in batch.contentByCID {
                    try validateExistingContent(cid: cid, data: data)
                }

                for volume in batch.volumes {
                    try insertMetadata(root: volume.root, entryCount: volume.entries.count)
                }
                for (cid, data) in batch.contentByCID {
                    try insertCASData(cid: cid, data: data)
                }
                for volume in batch.volumes {
                    for cid in volume.entries.keys {
                        try insertVolumeEntry(root: volume.root, cid: cid)
                    }
                }
                for volume in batch.volumes {
                    try verifyComplete(root: volume.root, entryCount: volume.entries.count)
                }
                try then?()
            }
        }
    }

    private struct ValidatedBatch: Sendable {
        let volumes: [SerializedVolume]
        let contentByCID: [String: Data]
    }

    private static func validateBatch(_ submitted: [ValidatedVolume]) throws -> ValidatedBatch {
        var memberships: [String: Set<String>] = [:]
        var contentByCID: [String: Data] = [:]
        var volumes: [SerializedVolume] = []

        for submittedVolume in submitted {
            let volume = submittedVolume.volume
            let members = Set(volume.entries.keys)
            if let existing = memberships[volume.root] {
                guard existing == members else {
                    throw BrokerError.conflictingVolume(volume.root)
                }
            } else {
                memberships[volume.root] = members
                volumes.append(volume)
            }

            for (cid, data) in volume.entries {
                if let existing = contentByCID[cid], existing != data {
                    throw BrokerError.conflictingContent(cid)
                }
                contentByCID[cid] = data
            }
        }
        return ValidatedBatch(volumes: volumes, contentByCID: contentByCID)
    }

    // MARK: - Transaction helpers

    private func validateExistingMembership(of volume: SerializedVolume) throws {
        let db = connection.db
        let existingEntryCount: Int? = try connection.withStatement(
            "SELECT entry_count FROM volume_metadata WHERE root = ?1"
        ) { metadataStmt in
            sqlite3_bind_text(metadataStmt, 1, volume.root, -1, SQLITE_TRANSIENT_SHIM)
            let metadataResult = sqlite3_step(metadataStmt)
            if metadataResult == SQLITE_ROW {
                guard sqlite3_column_type(metadataStmt, 0) == SQLITE_INTEGER else {
                    throw BrokerError.conflictingVolume(volume.root)
                }
                return Int(sqlite3_column_int64(metadataStmt, 0))
            } else if metadataResult == SQLITE_DONE {
                return nil
            } else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
        }

        let existingMembers: Set<String> = try connection.withStatement(
            "SELECT cid FROM volume_entries WHERE root = ?1"
        ) { membersStmt in
            sqlite3_bind_text(membersStmt, 1, volume.root, -1, SQLITE_TRANSIENT_SHIM)
            var existingMembers = Set<String>()
            while true {
                let result = sqlite3_step(membersStmt)
                if result == SQLITE_DONE { break }
                guard result == SQLITE_ROW, let cid = sqlite3_column_text(membersStmt, 0) else {
                    throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
                }
                existingMembers.insert(String(cString: cid))
            }
            return existingMembers
        }

        let expectedMembers = Set(volume.entries.keys)
        if let existingEntryCount {
            guard existingEntryCount == expectedMembers.count,
                  existingMembers == expectedMembers else {
                throw BrokerError.conflictingVolume(volume.root)
            }
        } else if !existingMembers.isEmpty {
            throw BrokerError.conflictingVolume(volume.root)
        }
    }

    private func validateExistingContent(cid: String, data: Data) throws {
        let db = connection.db
        // Nil when no row holds `cid`.
        let existing: Data? = try connection.withStatement("SELECT data FROM cas_data WHERE cid = ?1") { stmt in
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)

            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { return nil }
            guard result == SQLITE_ROW else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
            }
            guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else {
                throw BrokerError.sqlFailed("read existing content")
            }

            let length = Int(sqlite3_column_bytes(stmt, 0))
            if length == 0 {
                return Data()
            } else if let bytes = sqlite3_column_blob(stmt, 0) {
                return Data(bytes: bytes, count: length)
            } else {
                throw BrokerError.sqlFailed("read existing content")
            }
        }
        guard let existing, existing != data else { return }
        // Self-authenticating content is immutable; corrupt local bytes are repairable.
        if (try? SerializedVolume.validate(cid: cid, data: existing)) != nil {
            throw BrokerError.conflictingContent(cid)
        }
    }

    private func insertMetadata(root: String, entryCount: Int) throws {
        try connection.execBind(
            "INSERT OR IGNORE INTO volume_metadata(root, entry_count) VALUES(?1, ?2)"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_int64(stmt, 2, Int64(entryCount))
        }
    }

    private func insertCASData(cid: String, data: Data) throws {
        try connection.execBind(
            """
            INSERT INTO cas_data(cid, data) VALUES(?1, ?2)
            ON CONFLICT(cid) DO UPDATE SET data=excluded.data
            WHERE cas_data.data != excluded.data
            """
        ) { stmt in
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
            if data.isEmpty {
                sqlite3_bind_zeroblob(stmt, 2, 0)
            } else {
                _ = data.withUnsafeBytes { bytes in
                    sqlite3_bind_blob(stmt, 2, bytes.baseAddress, Int32(data.count), SQLITE_TRANSIENT_SHIM)
                }
            }
        }
    }

    private func insertVolumeEntry(root: String, cid: String) throws {
        try connection.execBind(
            "INSERT OR IGNORE INTO volume_entries(root, cid) VALUES(?1, ?2)"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT_SHIM)
        }
    }

    private func verifyComplete(root: String, entryCount: Int) throws {
        let db = connection.db
        let sql = """
            SELECT 1
            FROM volume_metadata vm
            WHERE vm.root = ?1
              AND vm.entry_count = ?2
              AND \(Self.completeVolumePredicate)
            LIMIT 1
            """
        try connection.withStatement(sql) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_int64(stmt, 2, Int64(entryCount))
            let result = sqlite3_step(stmt)
            if result == SQLITE_ROW { return }
            if result == SQLITE_DONE { throw BrokerError.conflictingVolume(root) }
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
    }
}
