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

/// When a write is durable is the opener's choice. Either way a write is
/// atomic and writes are ordered; what differs is whether a commit waits for
/// its fsync.
@Suite("Durability")
struct DurabilityTests {
    private func temporaryDatabase() throws -> (directory: URL, path: String) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerDurability-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return (directory, directory.appendingPathComponent("volumes.sqlite").path)
    }

    private func volume(_ name: String) -> SerializedVolume {
        let data = Data(name.utf8)
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        let root = try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
        return SerializedVolume(root: root, entries: [root: data])
    }

    private func synchronous(_ connection: SQLiteConnection) -> Int32 {
        var statement: OpaquePointer?
        sqlite3_prepare_v2(connection.db, "PRAGMA synchronous", -1, &statement, nil)
        defer { sqlite3_finalize(statement) }
        return sqlite3_step(statement) == SQLITE_ROW ? sqlite3_column_int(statement, 0) : -1
    }

    @Test func everyCommitIsTheDefaultAndSyncsEachCommit() throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: path)
        #expect(connection.durability == .everyCommit)
        #expect(synchronous(connection) == 2)
    }

    @Test func deferredCommitsDoNotWaitForTheirSync() throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: path, durability: .deferred)
        #expect(synchronous(connection) == 1)
    }

    @Test func aDeferredBrokerStoresReadsAndFlushes() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path, durability: .deferred)
        let stored = volume("deferred")
        try await broker.storeVolumesLocal([stored])
        #expect(await broker.fetchVolumeLocal(root: stored.root)?.entries == stored.entries)
        try await broker.flush()
        #expect(await broker.fetchVolumeLocal(root: stored.root)?.entries == stored.entries)
        // After a checkpoint the WAL holds nothing; a flush is still fine.
        await broker.checkpoint()
        try await broker.flush()
    }

    @Test func flushingAnEveryCommitBrokerDoesNothing() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path)
        let stored = volume("every-commit")
        try await broker.storeVolumesLocal([stored])
        try await broker.flush()
        #expect(await broker.fetchVolumeLocal(root: stored.root)?.entries == stored.entries)
    }

    /// The mode belongs to the opener, not the file: a store written one way
    /// opens the other way with everything in it.
    @Test func aStoreOpensInEitherModeWhateverWroteIt() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = volume("first"), second = volume("second")
        do {
            let broker = try DiskBroker(path: path, durability: .deferred)
            try await broker.storeVolumesLocal([first])
        }
        do {
            let broker = try DiskBroker(path: path)
            #expect(await broker.fetchVolumeLocal(root: first.root)?.entries == first.entries)
            try await broker.storeVolumesLocal([second])
        }
        let broker = try DiskBroker(path: path, durability: .deferred)
        #expect(await broker.fetchVolumeLocal(root: first.root)?.entries == first.entries)
        #expect(await broker.fetchVolumeLocal(root: second.root)?.entries == second.entries)
    }

    /// A process that dies loses nothing it committed, synced or not: the
    /// store's files as a crash would leave them, copied while the writer
    /// still holds them open and before any flush, open with every write.
    @Test func aDeferredStoreCopiedMidRunHoldsEveryCommittedWrite() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path, durability: .deferred)
        let volumes = (0..<50).map { volume("crash-\($0)") }
        for stored in volumes { try await broker.storeVolumesLocal([stored]) }

        let copy = directory.appendingPathComponent("copy")
        try FileManager.default.createDirectory(at: copy, withIntermediateDirectories: true)
        let copied = copy.appendingPathComponent("volumes.sqlite").path
        for suffix in ["", "-wal"] where FileManager.default.fileExists(atPath: path + suffix) {
            try FileManager.default.copyItem(atPath: path + suffix, toPath: copied + suffix)
        }
        let reopened = try DiskBroker(path: copied)
        for stored in volumes {
            #expect(await reopened.fetchVolumeLocal(root: stored.root)?.entries == stored.entries)
        }
    }
}
