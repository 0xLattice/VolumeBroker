import Foundation
import Testing
import cashew
import CID
import Multihash
@testable import VolumeBroker

@Suite("Cashew protocols")
struct CashewProtocolTests {
    typealias TestDictionary = VolumeMerkleDictionaryImpl<String>
    typealias TestVolume = VolumeImpl<TestDictionary>

    private enum InjectedFailure: Error {
        case store
    }

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private final class RecordingBroker: VolumeBroker, @unchecked Sendable {
        let near: (any VolumeBroker)? = nil
        let far: (any VolumeBroker)? = nil
        private let lock = NSLock()
        private var scalarReads = 0
        private var batchReads = 0

        func hasVolume(root: String) async -> Bool { false }
        func fetchVolumeLocal(root: String) async -> SerializedVolume? { nil }
        func fetchDataLocal(cid: String) async -> Data? {
            lock.withLock { scalarReads += 1 }
            return nil
        }
        func fetchDataLocal(cids: Set<String>) async -> [String: Data] {
            lock.withLock { batchReads += 1 }
            return Dictionary(uniqueKeysWithValues: cids.map { ($0, Data($0.utf8)) })
        }
        func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {}

        var counts: (scalar: Int, batch: Int) {
            lock.withLock { (scalarReads, batchReads) }
        }
    }

    private final class FlakyBroker: VolumeBroker, @unchecked Sendable {
        let near: (any VolumeBroker)? = nil
        let far: (any VolumeBroker)? = nil
        var failNextStore = true
        private(set) var attempts = 0
        let backing = MemoryBroker()

        func hasVolume(root: String) async -> Bool {
            await backing.hasVolume(root: root)
        }
        func fetchVolumeLocal(root: String) async -> SerializedVolume? {
            await backing.fetchVolumeLocal(root: root)
        }
        func storeVolumesLocal(_ volumes: [SerializedVolume]) async throws {
            attempts += 1
            if failNextStore {
                failNextStore = false
                throw InjectedFailure.store
            }
            try await backing.storeVolumesLocal(volumes)
        }
    }

    @Test func brokerIsTheCashewBoundary() {
        let broker = MemoryBroker()

        #expect((broker as Any) is any VolumeStorer)
        #expect((broker as Any) is any ContentSource)
        #expect((broker as Any) is any Fetcher)
        #expect(!((broker as Any) is any Storer))
    }

    @Test func missingPerCIDFetchPreservesBrokerError() async {
        do {
            _ = try await MemoryBroker().fetch(rawCid: "missing")
            Issue.record("expected missing CID fetch to fail")
        } catch {
            #expect(error as? BrokerError == .notFound)
        }
    }

    @Test func failedStoreCanBeRetriedWithoutAdapterState() async throws {
        let broker = FlakyBroker()
        let data = Data("retry".utf8)
        let root = cid(for: data)
        let volume = SerializedVolume(root: root, entries: [root: data])

        do {
            try await broker.store(volume: volume)
            Issue.record("expected the first store to fail")
        } catch {
            #expect(error as? InjectedFailure == .store)
        }
        #expect(await broker.hasVolume(root: root) == false)

        try await broker.store(volume: volume)
        #expect(await broker.hasVolume(root: root))
        #expect(broker.attempts == 2)
    }

    @Test func resolvesMultiNodeVolumeThroughBatchedSource() async throws {
        let broker = MemoryBroker()
        let dictionary = try TestDictionary()
            .inserting(key: "alice", value: "v1")
            .inserting(key: "alicia", value: "v2")
            .inserting(key: "bob", value: "v3")
        let original = try TestVolume(node: dictionary)

        try await original.storeRecursively(storer: broker)

        let lazyRoot = TestVolume(rawCID: original.rawCID, node: nil, encryptionInfo: nil)
        let resolved = try await lazyRoot.resolveRecursive(source: broker)

        #expect(try resolved.node?.get(key: "alice") == "v1")
        #expect(try resolved.node?.get(key: "alicia") == "v2")
        #expect(try resolved.node?.get(key: "bob") == "v3")
    }

    /// Cashew ends a Volume at each nested Volume's boundary, so the outer
    /// Volume's entries do not hold the inner root: nothing but the inner
    /// root's own retention keeps it.
    @Test func sweepKeepsANestedVolumeOnlyWhenItsOwnRootIsRetained() async throws {
        typealias Outer = VolumeImpl<MerkleDictionaryImpl<TestVolume>>
        let inner = try TestVolume(node: TestDictionary().inserting(key: "k", value: "v"))
        let outer = try Outer(node: MerkleDictionaryImpl<TestVolume>().inserting(key: "inner", value: inner))

        for retainInner in [true, false] {
            let broker = MemoryBroker()
            try await outer.storeRecursively(storer: broker)
            #expect(await broker.fetchVolumeLocal(root: outer.rawCID)?.entries[inner.rawCID] == nil)

            try await broker.advanceRetainedRoots(
                scope: "s", roots: retainInner ? [outer.rawCID, inner.rawCID] : [outer.rawCID])
            try await broker.sweep()

            #expect(await broker.hasVolume(root: outer.rawCID))
            #expect(await broker.hasVolume(root: inner.rawCID) == retainInner)
            #expect((await broker.fetchDataLocal(cid: inner.rawCID) != nil) == retainInner)
        }
    }

    @Test func batchedFetchReturnsRequestedVolumeRoots() async throws {
        let broker = MemoryBroker()
        let a = try TestVolume(node: TestDictionary().inserting(key: "k", value: "va"))
        let b = try TestVolume(node: TestDictionary().inserting(key: "k", value: "vb"))
        try await a.storeRecursively(storer: broker)
        try await b.storeRecursively(storer: broker)

        let got = await broker.fetch([a.rawCID, b.rawCID, "missing"])
        #expect(got[a.rawCID] != nil)
        #expect(got[b.rawCID] != nil)
        #expect(got["missing"] == nil)
    }

    @Test func batchedFetchUsesOneBrokerBatch() async {
        let broker = RecordingBroker()
        let got = await broker.fetch(["a", "b", "c"])

        #expect(got == [
            "a": Data("a".utf8),
            "b": Data("b".utf8),
            "c": Data("c".utf8),
        ])
        #expect(broker.counts.scalar == 0)
        #expect(broker.counts.batch == 1)
    }

    @Test func fetchesInternalEntryByCidWithoutEnteringVolume() async throws {
        // Object-grain storage: one Volume holds multiple entries; the
        // internal entry is NOT its own volume root. With the cas_data CID-blob
        // primitive it resolves directly by CID.
        let broker = MemoryBroker()
        let rootData = Data("root".utf8)
        let internalData = Data("internal-data".utf8)
        let root = cid(for: rootData)
        let internalCID = cid(for: internalData)
        try await broker.store(volume: SerializedVolume(
            root: root,
            entries: [root: rootData, internalCID: internalData]
        ))

        // No Volume is keyed by the internal CID; it is only an entry under root.
        #expect(await broker.fetchVolumeLocal(root: internalCID) == nil)

        let data = try await broker.fetch(rawCid: internalCID)
        #expect(data == internalData)

        let batched = await broker.fetch([root, internalCID, "missing"])
        #expect(batched[root] == rootData)
        #expect(batched[internalCID] == internalData)
        #expect(batched["missing"] == nil)
    }
}
