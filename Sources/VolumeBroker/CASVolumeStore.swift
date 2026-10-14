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

    static func isCompleteVolume(root: String, db: OpaquePointer) throws -> Bool {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT 1
            FROM volume_metadata vm
            WHERE vm.root = ?1
              AND \(completeVolumePredicate)
            LIMIT 1
            """
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        let result = sqlite3_step(stmt)
        if result == SQLITE_ROW { return true }
        if result == SQLITE_DONE { return false }
        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(db)))
    }

    /// A Volume with any CID-invalid member is not served; a valid re-store
    /// overwrites the corrupt bytes.
    func fetchVolumeLocal(root: String) async -> SerializedVolume? {
        await connection.read {
            (try? Self.loadValidatedVolume(root: root, db: connection.readDb)) ?? nil
        }
    }

    static func loadValidatedVolume(root: String, db: OpaquePointer) throws -> SerializedVolume? {
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
        guard !entries.isEmpty,
              entries.allSatisfy({ (try? SerializedVolume.validate(cid: $0.key, data: $0.value)) != nil })
        else { return nil }
        return SerializedVolume(root: root, entries: entries)
    }

    /// A CAS row is readable only through at least one complete owning Volume.
    func fetchDataLocal(cid: String) async -> Data? {
        await fetchDataLocal(cids: [cid])[cid]
    }

    /// A batched point read preserves the same serve gate and CID validation as
    /// scalar reads while using bounded SQLite parameter sets.
    func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
        guard !cids.isEmpty else { return [:] }
        let loaded: [String: Data]? = await connection.read {
            try? Self.loadServableData(cids: cids, db: connection.readDb)
        }
        guard let loaded else { return [:] }
        return loaded.filter { cid, data in
            (try? SerializedVolume.validate(cid: cid, data: data)) != nil
        }
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
        let batch = try Self.validateBatch(volumes)
        guard !batch.volumes.isEmpty else { return }

        try await connection.write {
            try connection.transaction {
                // A later store of a root replaces its entry list: only the
                // root's own bytes are verified on receipt, so a first answer
                // is never final.
                var droppedByRoot: [String: Set<String>] = [:]
                for volume in batch.volumes {
                    droppedByRoot[volume.root] = try existingMembers(root: volume.root)
                        .subtracting(volume.entries.keys)
                }
                for (cid, data) in batch.contentByCID {
                    try validateExistingContent(cid: cid, data: data)
                }

                for volume in batch.volumes {
                    try upsertMetadata(root: volume.root, entryCount: volume.entries.count)
                }
                for (cid, data) in batch.contentByCID {
                    try insertCASData(cid: cid, data: data)
                }
                for volume in batch.volumes {
                    for cid in droppedByRoot[volume.root] ?? [] {
                        try deleteVolumeEntry(root: volume.root, cid: cid)
                    }
                    for cid in volume.entries.keys {
                        try insertVolumeEntry(root: volume.root, cid: cid)
                    }
                }
                // Reclaimed as a release reclaims: a dropped CID goes only
                // when no Volume lists it any more.
                for cid in Set(droppedByRoot.values.joined()) {
                    try deleteUnownedCASData(cid: cid)
                }
                for volume in batch.volumes {
                    try verifyComplete(root: volume.root, entryCount: volume.entries.count)
                }
            }
        }
    }

    private struct ValidatedBatch: Sendable {
        let volumes: [SerializedVolume]
        let contentByCID: [String: Data]
    }

    /// Within one batch, too, a later Volume for a root replaces an earlier one.
    private static func validateBatch(_ submitted: [SerializedVolume]) throws -> ValidatedBatch {
        var indexByRoot: [String: Int] = [:]
        var contentByCID: [String: Data] = [:]
        var volumes: [SerializedVolume] = []

        for submittedVolume in submitted {
            let volume = submittedVolume.ownedCopy()
            try volume.validate()
            if let index = indexByRoot[volume.root] {
                volumes[index] = volume
            } else {
                indexByRoot[volume.root] = volumes.count
                volumes.append(volume)
            }
        }
        for volume in volumes {
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

    private func existingMembers(root: String) throws -> Set<String> {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(
            connection.db,
            "SELECT cid FROM volume_entries WHERE root = ?1",
            -1,
            &stmt,
            nil
        ) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)

        var members = Set<String>()
        while true {
            let result = sqlite3_step(stmt)
            if result == SQLITE_DONE { break }
            guard result == SQLITE_ROW, let cid = sqlite3_column_text(stmt, 0) else {
                throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
            }
            members.insert(String(cString: cid))
        }
        return members
    }

    private func validateExistingContent(cid: String, data: Data) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_prepare_v2(
            connection.db,
            "SELECT data FROM cas_data WHERE cid = ?1",
            -1,
            &stmt,
            nil
        ) == SQLITE_OK, let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)

        let result = sqlite3_step(stmt)
        if result == SQLITE_DONE { return }
        guard result == SQLITE_ROW else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        guard sqlite3_column_type(stmt, 0) != SQLITE_NULL else {
            throw BrokerError.sqlFailed("read existing content")
        }

        let length = Int(sqlite3_column_bytes(stmt, 0))
        let existing: Data
        if length == 0 {
            existing = Data()
        } else if let bytes = sqlite3_column_blob(stmt, 0) {
            existing = Data(bytes: bytes, count: length)
        } else {
            throw BrokerError.sqlFailed("read existing content")
        }
        guard existing != data else { return }
        // Self-authenticating content is immutable; corrupt local bytes are repairable.
        if (try? SerializedVolume.validate(cid: cid, data: existing)) != nil {
            throw BrokerError.conflictingContent(cid)
        }
    }

    /// Updates in place, never deletes, so retained rows naming the root stay.
    private func upsertMetadata(root: String, entryCount: Int) throws {
        try connection.execBind(
            """
            INSERT INTO volume_metadata(root, entry_count) VALUES(?1, ?2)
            ON CONFLICT(root) DO UPDATE SET entry_count=excluded.entry_count
            """
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

    private func deleteVolumeEntry(root: String, cid: String) throws {
        try connection.execBind(
            "DELETE FROM volume_entries WHERE root=?1 AND cid=?2"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
            sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT_SHIM)
        }
    }

    private func deleteUnownedCASData(cid: String) throws {
        try connection.execBind(
            "DELETE FROM cas_data WHERE cid=?1 AND NOT EXISTS (SELECT 1 FROM volume_entries WHERE cid=?1)"
        ) { stmt in
            sqlite3_bind_text(stmt, 1, cid, -1, SQLITE_TRANSIENT_SHIM)
        }
    }

    private func verifyComplete(root: String, entryCount: Int) throws {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        let sql = """
            SELECT 1
            FROM volume_metadata vm
            WHERE vm.root = ?1
              AND vm.entry_count = ?2
              AND \(Self.completeVolumePredicate)
            LIMIT 1
            """
        guard sqlite3_prepare_v2(connection.db, sql, -1, &stmt, nil) == SQLITE_OK,
              let stmt else {
            throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
        }
        sqlite3_bind_text(stmt, 1, root, -1, SQLITE_TRANSIENT_SHIM)
        sqlite3_bind_int64(stmt, 2, Int64(entryCount))
        let result = sqlite3_step(stmt)
        if result == SQLITE_ROW { return }
        if result == SQLITE_DONE { throw BrokerError.inconsistentState("Volume \(root) did not commit whole") }
        throw BrokerError.sqlFailed(String(cString: sqlite3_errmsg(connection.db)))
    }
}
