import Testing
import Foundation
@testable import VolumeBroker

/// Only a Volume's root is verified on receipt, so a peer's first answer is
/// never final: a later store of the root replaces its entry list.
@Suite("Entry-list replacement")
struct ReplaceTests {
    typealias P = ScopeTests

    private static let scope = "bafychain:Nexus"
    private static let correct = P.volume("block", ["tx-a", "tx-b"])
    private static let padded = P.volume("block", ["tx-a", "tx-b", "pad-1", "pad-2"])
    private static let padding = ["pad-1", "pad-2"].map(P.cid)

    /// Disk opened so reads run on the read-only connection.
    private static func open(_ kind: P.Kind) async throws -> P.Store {
        kind == .disk ? try await P.openDisk(.before) : try P.open(.memory)
    }

    private static func expectHolds(_ store: P.Store, exactly volumes: [SerializedVolume]) async throws {
        #expect(try await P.apiProblems(store.broker, universe: volumes) == [])
        if let path = store.path {
            #expect(P.diskProblems(path) == [])
            let entries = Set(volumes.flatMap(\.entries.keys))
            #expect(P.tableCounts(path) == [
                volumes.count,
                volumes.reduce(0) { $0 + $1.entries.count },
                entries.count,
                volumes.count,
            ])
        }
    }

    @Test(arguments: P.Kind.allCases)
    func aCorrectStoreReplacesAPaddedOneAndReclaimsThePadding(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        try await store.broker.store(volume: Self.padded)
        try await store.broker.mergeRetainedRoots(scope: Self.scope, roots: [Self.correct.root])
        try await store.broker.store(volume: Self.correct)

        #expect(await store.broker.fetchVolumeLocal(root: Self.correct.root)?.entries == Self.correct.entries)
        #expect(await store.broker.fetchDataLocal(cids: Set(Self.padding)).isEmpty)
        try await Self.expectHolds(store, exactly: [Self.correct])
    }

    @Test(arguments: P.Kind.allCases)
    func aLaterVolumeInOneBatchReplacesAnEarlierOne(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        try await store.broker.storeVolumesLocal([Self.padded, Self.correct])
        try await store.broker.mergeRetainedRoots(scope: Self.scope, roots: [Self.correct.root])

        #expect(await store.broker.fetchVolumeLocal(root: Self.correct.root)?.entries == Self.correct.entries)
        #expect(await store.broker.fetchDataLocal(cids: Set(Self.padding)).isEmpty)
        try await Self.expectHolds(store, exactly: [Self.correct])
    }

    @Test(arguments: P.Kind.allCases)
    func aPinnedRootStaysPinnedAndWholeThroughSubsetsAndSupersets(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        let small = P.volume("block", ["tx-a"])
        let large = P.volume("block", ["tx-a", "tx-b", "tx-c"])
        try await store.broker.store(volume: Self.correct)
        try await store.broker.mergeRetainedRoots(scope: Self.scope, roots: [Self.correct.root])

        for volume in [small, large, Self.correct, large, small] {
            try await store.broker.store(volume: volume)
            #expect(try await store.broker.retainedRoots(scope: Self.scope) == [volume.root])
            #expect(await store.broker.hasVolume(root: volume.root))
            #expect(await store.broker.fetchVolumeLocal(root: volume.root)?.entries == volume.entries)
            try await Self.expectHolds(store, exactly: [volume])
        }

        // Still one Volume to release, which reclaims everything.
        #expect(try await store.broker.releaseRetainedRoots(scope: Self.scope, roots: [small.root]) == 1)
        try await Self.expectHolds(store, exactly: [])
    }

    @Test(arguments: P.Kind.allCases)
    func anEntryAnotherVolumeListsSurvivesBeingDropped(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        let other = P.volume("other", ["shared"])
        let before = P.volume("block", ["shared", "tx-a"])
        let after = P.volume("block", ["tx-a"])
        try await store.broker.storeVolumesLocal([other, before])
        try await store.broker.mergeRetainedRoots(scope: Self.scope, roots: [other.root, before.root])
        try await store.broker.store(volume: after)

        #expect(await store.broker.fetchVolumeLocal(root: other.root)?.entries == other.entries)
        #expect(await store.broker.fetchDataLocal(cid: P.cid("shared")) == Data("shared".utf8))
        try await Self.expectHolds(store, exactly: [other, after])

        // And goes with the last Volume that lists it.
        try await store.broker.releaseRetainedRoots(scope: Self.scope, roots: [other.root])
        #expect(await store.broker.fetchDataLocal(cid: P.cid("shared")) == nil)
        try await Self.expectHolds(store, exactly: [after])
    }

    @Test(arguments: P.Kind.allCases)
    func theSameListChangesNothing(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        try await store.broker.store(volume: Self.correct)
        try await store.broker.mergeRetainedRoots(scope: Self.scope, roots: [Self.correct.root])
        try await store.broker.store(volume: Self.correct)
        try await Self.expectHolds(store, exactly: [Self.correct])
    }

    private actor Flag {
        var raised = false
        var seen: [String] = []
        func raise() { raised = true }
        func record(_ problem: String) { seen.append(problem) }
    }

    /// A reader sees the old list or the new one, never a mix or nothing.
    @Test(arguments: P.Kind.allCases)
    func readersNeverSeeAPartialList(kind: P.Kind) async throws {
        let store = try await Self.open(kind)
        let broker = store.broker
        let lists = [Self.correct, Self.padded, P.volume("block", ["tx-c"])]
        let allowed = lists.map(\.entries)
        let rounds = kind == .disk ? 150 : 1500
        try await broker.store(volume: lists[0])
        try await broker.mergeRetainedRoots(scope: Self.scope, roots: [Self.correct.root])
        let done = Flag()

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                for round in 0..<rounds {
                    try await broker.store(volume: lists[round % lists.count])
                }
                await done.raise()
            }
            for _ in 0..<4 {
                group.addTask {
                    while await !done.raised {
                        guard let entries = await broker.fetchVolumeLocal(root: Self.correct.root)?.entries else {
                            await done.record("absent"); continue
                        }
                        if !allowed.contains(entries) { await done.record("partial: \(entries.count) entries") }
                        await Task.yield()
                    }
                }
            }
            try await group.waitForAll()
        }

        #expect(await done.seen == [])
        try await Self.expectHolds(store, exactly: [lists[(rounds - 1) % lists.count]])
    }
}
