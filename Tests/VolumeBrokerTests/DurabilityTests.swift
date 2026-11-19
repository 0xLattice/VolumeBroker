import Testing
import Foundation
import CID
import Multihash
@testable import VolumeBroker

/// When a write is durable is the opener's choice. What an operator finds of
/// it: a deferred store reads and flushes as any other, a store opens in
/// either mode whatever wrote it, a process that dies loses nothing it
/// committed, and after a power cut (`PowerLossDisk`) a store holds what was
/// flushed, whole, and opens.
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

    // MARK: - After a power cut

    /// Stores `count` Volumes one at a time, in order, and returns them.
    private func store(_ count: Int, named name: String, in broker: DiskBroker) async throws -> [SerializedVolume] {
        var stored: [SerializedVolume] = []
        for index in 0..<count {
            let next = volume("\(name)-\(index)")
            try await broker.storeVolumesLocal([next])
            stored.append(next)
        }
        return stored
    }

    /// What a reopened store holds of `volumes`, in the order they were
    /// written: true where the Volume is there, whole.
    private func held(_ volumes: [SerializedVolume], in broker: DiskBroker) async -> [Bool] {
        var held: [Bool] = []
        for volume in volumes {
            let found = await broker.fetchVolumeLocal(root: volume.root)
            #expect(found == nil || found?.entries == volume.entries, "a Volume is whole or absent")
            held.append(found != nil)
        }
        return held
    }

    @Test func afterAPowerCutADeferredStoreHoldsEverythingWrittenBeforeItsFlush() async throws {
        PowerLossDisk.install()
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path, durability: .deferred, vfs: PowerLossDisk.vfsName)
        let flushed = try await store(40, named: "flushed", in: broker)
        try await broker.flush()
        let later = try await store(40, named: "later", in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        #expect(await held(flushed, in: reopened) == Array(repeating: true, count: 40), "flushed, so still there")
        // What came after the flush may be gone, never out of order: a
        // Volume is there only if every one written before it is.
        let kept = await held(later, in: reopened)
        #expect(kept == kept.sorted { $0 && !$1 }, "the store holds a prefix of what was written")
    }

    @Test func afterAPowerCutADeferredStoreNeverFlushedOpensWithAPrefixOfItsWrites() async throws {
        PowerLossDisk.install()
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path, durability: .deferred, vfs: PowerLossDisk.vfsName)
        let written = try await store(60, named: "unflushed", in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        let kept = await held(written, in: reopened)
        #expect(kept == kept.sorted { $0 && !$1 }, "the store holds a prefix of what was written")
        #expect(kept.contains(false), "nothing was flushed: the power cut lost the newest writes")
        // And it is a working store: it takes the lost writes again.
        try await reopened.storeVolumesLocal(written.map { $0 })
        #expect(await held(written, in: reopened) == Array(repeating: true, count: 60))
    }

    @Test func afterAPowerCutAnEveryCommitStoreHoldsEveryWriteThatReturned() async throws {
        PowerLossDisk.install()
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path, durability: .everyCommit, vfs: PowerLossDisk.vfsName)
        let written = try await store(40, named: "synced", in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        #expect(await held(written, in: reopened) == Array(repeating: true, count: 40))
    }
}
