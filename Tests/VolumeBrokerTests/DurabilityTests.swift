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

    /// A caller may flush whatever the mode: on a store that syncs every
    /// commit it changes nothing.
    @Test func flushingAStoreThatSyncsEveryCommitChangesNothing() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path)
        let stored = volume("synced")
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

    // MARK: - After a power cut

    /// A store on a disk that can lose power, opened by `open` as a caller
    /// opens one.
    private func onPowerLossDisk(_ open: () throws -> DiskBroker) rethrows -> DiskBroker {
        PowerLossDisk.install()
        return try SQLiteConnection.$vfs.withValue(PowerLossDisk.vfsName, operation: open)
    }

    /// Stores `count` Volumes of `size` bytes one at a time, in order, and
    /// returns them.
    private func store(
        _ count: Int, named name: String, size: Int = 0, in broker: DiskBroker
    ) async throws -> [SerializedVolume] {
        var stored: [SerializedVolume] = []
        for index in 0..<count {
            var data = Data("\(name)-\(index)".utf8)
            data.append(Data(repeating: UInt8(truncatingIfNeeded: index), count: size))
            let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
            let root = try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
            let next = SerializedVolume(root: root, entries: [root: data])
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

    /// Whether what is held is a prefix of what was written: a Volume is
    /// there only if every one written before it is.
    private func isPrefix(_ held: [Bool]) -> Bool {
        !held.drop { $0 }.contains(true)
    }

    /// The README's promise for a store opened with nothing said: a store
    /// that returned survives a power cut.
    @Test func afterAPowerCutAStoreOpenedByDefaultHoldsEveryWriteThatReturned() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try onPowerLossDisk { try DiskBroker(path: path) }
        let written = try await store(40, named: "default", in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        #expect(await held(written, in: reopened) == Array(repeating: true, count: 40))
    }

    @Test func afterAPowerCutADeferredStoreHoldsEverythingWrittenBeforeItsFlush() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try onPowerLossDisk { try DiskBroker(path: path, durability: .deferred) }
        let flushed = try await store(40, named: "flushed", in: broker)
        try await broker.flush()
        let later = try await store(40, named: "later", in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        #expect(await held(flushed, in: reopened) == Array(repeating: true, count: 40), "flushed, so still there")
        // What came after the flush may be gone, never out of order.
        #expect(isPrefix(await held(later, in: reopened)), "the store holds a prefix of what was written")
    }

    /// Enough is written, with no flush, for the store to have synced part
    /// of it on its own: what a power cut leaves is some of the writes and
    /// not all, the oldest ones, each whole, in a store that takes the rest
    /// again.
    @Test func afterAPowerCutADeferredStoreNeverFlushedHoldsAPrefixOfItsWrites() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try onPowerLossDisk { try DiskBroker(path: path, durability: .deferred) }
        let written = try await store(24, named: "unflushed", size: 256 * 1_024, in: broker)

        let after = try PowerLossDisk.afterOutage(of: path, into: directory.appendingPathComponent("after"))
        let reopened = try DiskBroker(path: after)
        let kept = await held(written, in: reopened)
        // The premise, or the prefix compared is empty: part of what was
        // written was kept.
        #expect(kept.contains(true), "nothing was kept: no prefix to check")
        #expect(isPrefix(kept), "the store holds a prefix of what was written")
        try await reopened.storeVolumesLocal(written)
        #expect(await held(written, in: reopened) == Array(repeating: true, count: written.count))
    }

    // MARK: - The files an operator sees

    /// The `-wal` file beside the store does not stay as large as the
    /// largest write made it: after one write of more than 16 MiB and a few
    /// small ones, it is under 8 MiB.
    @Test func theWALFileDoesNotStayTheSizeOfTheLargestWrite() async throws {
        let (directory, path) = try temporaryDatabase()
        defer { try? FileManager.default.removeItem(at: directory) }
        let broker = try DiskBroker(path: path)
        func walBytes() -> Int {
            ((try? FileManager.default.attributesOfItem(atPath: path + "-wal")[.size]) as? Int) ?? 0
        }
        var entries: [String: Data] = [:]
        for index in 0..<17 {
            var data = Data("large-\(index)".utf8)
            data.append(Data(repeating: UInt8(index), count: 1 << 20))
            let multihash = try Multihash(raw: data, hashedWith: .sha2_256)
            entries[try CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString] = data
        }
        let root = try #require(entries.keys.sorted().first)
        try await broker.storeVolumesLocal([SerializedVolume(root: root, entries: entries)])
        // The premise: the write did make the file large.
        #expect(walBytes() > 16 << 20, "the -wal file is \(walBytes() >> 20) MiB after the large write")
        _ = try await store(8, named: "small", in: broker)
        #expect(walBytes() < 8 << 20, "the -wal file is \(walBytes() >> 20) MiB")
    }
}
