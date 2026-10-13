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

/// Retention and sweep, run against both brokers.
@Suite("Retention")
struct RetentionTests {

    enum Kind: String, CaseIterable, Sendable {
        case memory
        case disk
    }

    private static func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private static func cid(_ label: String) -> String {
        cid(for: Data(label.utf8))
    }

    /// A Volume rooted at `label` whose entries also hold `members` (each a
    /// label whose bytes are the label). A member that is itself stored as a
    /// Volume root is a nested Volume.
    private static func volume(_ label: String, _ members: [String] = []) -> SerializedVolume {
        var entries = [cid(label): Data(label.utf8)]
        for member in members { entries[cid(member)] = Data(member.utf8) }
        return SerializedVolume(root: cid(label), entries: entries)
    }

    private static func broker(_ kind: Kind) throws -> any RetainedRootReleaseBroker {
        switch kind {
        case .memory:
            return MemoryBroker()
        case .disk:
            return try DiskBroker(path: NSTemporaryDirectory() + "vb_retention_\(UUID().uuidString).sqlite")
        }
    }

    @Test(arguments: Kind.allCases)
    func aRetainedRootKeepsItsOwnVolumeAndNothingNestedUnderIt(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([
            Self.volume("top", ["mid", "top-leaf"]),
            Self.volume("mid", ["bottom", "mid-leaf"]),
            Self.volume("bottom", ["bottom-leaf"]),
            Self.volume("stray", ["stray-leaf"]),
        ])
        // `mid` is nested under a retained root and holds another: neither keeps it.
        try await broker.advanceRetainedRoots(scope: "s", roots: [Self.cid("top"), Self.cid("bottom")])

        #expect(try await broker.sweep() == 2)
        for root in ["top", "bottom"] {
            #expect(await broker.hasVolume(root: Self.cid(root)), "\(root)")
        }
        for root in ["mid", "stray"] {
            #expect(await broker.hasVolume(root: Self.cid(root)) == false, "\(root)")
        }
        // A kept Volume keeps its own entries, a nested root's bytes among them.
        for kept in ["top-leaf", "mid", "bottom-leaf"] {
            #expect(await broker.fetchDataLocal(cid: Self.cid(kept)) == Data(kept.utf8), "\(kept)")
        }
        for swept in ["mid-leaf", "stray-leaf"] {
            #expect(await broker.fetchDataLocal(cid: Self.cid(swept)) == nil, "\(swept)")
        }
    }

    @Test(arguments: Kind.allCases)
    func advancingSweepsWhatTheNewSetDoesNotName(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([
            Self.volume("old", ["shared", "old-branch"]),
            Self.volume("shared", ["shared-leaf"]),
            Self.volume("old-branch", ["old-leaf"]),
        ])
        try await broker.advanceRetainedRoots(
            scope: "s", roots: ["old", "shared", "old-branch"].map(Self.cid))
        #expect(try await broker.sweep() == 0)

        try await broker.storeVolumesLocal([
            Self.volume("new", ["shared", "new-branch"]),
            Self.volume("new-branch", ["new-leaf"]),
        ])
        try await broker.advanceRetainedRoots(
            scope: "s", roots: ["new", "shared", "new-branch"].map(Self.cid))
        #expect(try await broker.sweep() == 2)

        for root in ["new", "shared", "new-branch"] {
            #expect(await broker.hasVolume(root: Self.cid(root)), "\(root)")
        }
        #expect(await broker.fetchDataLocal(cid: Self.cid("shared-leaf")) == Data("shared-leaf".utf8))
        for root in ["old", "old-branch"] {
            #expect(await broker.hasVolume(root: Self.cid(root)) == false, "\(root)")
        }
        #expect(await broker.fetchDataLocal(cid: Self.cid("old-leaf")) == nil)
    }

    @Test(arguments: Kind.allCases)
    func releasingRemovesTheNamedVolumesNoScopeRetainsAndNothingElse(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([
            Self.volume("gone", ["gone-leaf", "shared-leaf"]),
            Self.volume("elsewhere", ["elsewhere-leaf"]),
            Self.volume("kept", ["shared-leaf"]),
            Self.volume("stray", ["stray-leaf"]),
        ])
        try await broker.advanceRetainedRoots(scope: "a", roots: ["gone", "elsewhere", "kept"].map(Self.cid))
        try await broker.advanceRetainedRoots(scope: "b", roots: [Self.cid("elsewhere")])

        // `absent` was never stored and `stray` is in no scope: neither is an error.
        let removed = try await broker.releaseRetainedRoots(
            scope: "a", roots: ["gone", "elsewhere", "absent"].map(Self.cid))

        #expect(removed == 1)
        #expect(try await broker.retainedRoots(scope: "a") == [Self.cid("kept")])
        #expect(try await broker.retainedRoots(scope: "b") == [Self.cid("elsewhere")])
        #expect(await broker.hasVolume(root: Self.cid("gone")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("gone-leaf")) == nil)
        // Another scope still retains it.
        #expect(await broker.hasVolume(root: Self.cid("elsewhere")))
        // Bytes a surviving Volume owns stay.
        #expect(await broker.fetchDataLocal(cid: Self.cid("shared-leaf")) == Data("shared-leaf".utf8))
        // Unlike a sweep, content that was not named is not touched, retained or not.
        #expect(await broker.hasVolume(root: Self.cid("stray")))
        #expect(await broker.hasVolume(root: Self.cid("kept")))
    }

    @Test(arguments: Kind.allCases)
    func scopesAreListedByPrefixWhileTheyRetainSomething(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("one"), Self.volume("two")])
        try await broker.advanceRetainedRoots(scope: "body:b", roots: [Self.cid("one")])
        try await broker.advanceRetainedRoots(scope: "body:a", roots: [Self.cid("one"), Self.cid("two")])
        try await broker.advanceRetainedRoots(scope: "bodyguard", roots: [Self.cid("two")])
        try await broker.advanceRetainedRoots(scope: "state", roots: [Self.cid("two")])
        try await broker.advanceRetainedRoots(scope: "body:%", roots: [Self.cid("two")])

        #expect(try await broker.retainedScopes(prefix: "body:") == ["body:%", "body:a", "body:b"])
        #expect(try await broker.retainedScopes(prefix: "body") == ["body:%", "body:a", "body:b", "bodyguard"])
        #expect(try await broker.retainedScopes(prefix: "") == ["body:%", "body:a", "body:b", "bodyguard", "state"])
        #expect(try await broker.retainedScopes(prefix: "none") == [])

        // A scope whose whole name is the prefix is listed.
        try await broker.advanceRetainedRoots(scope: "body", roots: [Self.cid("one")])
        #expect(try await broker.retainedScopes(prefix: "body").first == "body")
        try await broker.advanceRetainedRoots(scope: "body", roots: [])

        // A scope that retains nothing is not a scope any more.
        try await broker.releaseRetainedRoots(scope: "body:b", roots: [Self.cid("one")])
        try await broker.advanceRetainedRoots(scope: "body:%", roots: [])
        #expect(try await broker.retainedScopes(prefix: "body:") == ["body:a"])
    }

    // MARK: - One scope per owner, as a chain node keeps them

    /// A block's body is its own Volume and its transactions'. Two blocks
    /// that carry one transaction each name it, so it goes with the second.
    @Test(arguments: Kind.allCases)
    func aRootTwoOwnersNameGoesWithTheSecond(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal(
            ["block-a", "block-b", "shared-tx", "only-a-tx", "state"].map { Self.volume($0, ["\($0)-leaf"]) })
        let (a, b) = ("chain:body:block-a", "chain:body:block-b")
        try await broker.mergeRetainedRoots(scope: a, roots: ["block-a", "shared-tx", "only-a-tx"].map(Self.cid))
        try await broker.mergeRetainedRoots(scope: b, roots: ["block-b", "shared-tx"].map(Self.cid))
        try await broker.mergeRetainedRoots(scope: "chain", roots: [Self.cid("state")])

        // Letting go of a block is releasing everything its scope retains.
        let removed = try await broker.releaseRetainedRoots(scope: a, roots: try await broker.retainedRoots(scope: a))
        #expect(removed == 2)
        #expect(try await broker.retainedScopes(prefix: "chain:body:") == [b])
        for gone in ["block-a", "only-a-tx"] {
            #expect(await broker.hasVolume(root: Self.cid(gone)) == false, "\(gone)")
            #expect(await broker.fetchDataLocal(cid: Self.cid("\(gone)-leaf")) == nil, "\(gone)")
        }
        for kept in ["block-b", "shared-tx", "state"] {
            #expect(await broker.hasVolume(root: Self.cid(kept)), "\(kept)")
        }
        // The chain's own scope is not a body scope.
        #expect(try await broker.retainedRoots(scope: "chain") == [Self.cid("state")])

        #expect(try await broker.releaseRetainedRoots(scope: b, roots: try await broker.retainedRoots(scope: b)) == 2)
        #expect(await broker.hasVolume(root: Self.cid("shared-tx")) == false)
        #expect(try await broker.retainedScopes(prefix: "chain:body:") == [])
        #expect(await broker.hasVolume(root: Self.cid("state")))
    }

    /// A store written before bodies had scopes of their own names a body
    /// in the chain's scope too: releasing the block's scope keeps it.
    @Test(arguments: Kind.allCases)
    func aBodyTheChainScopeAlsoNamesStaysWhenItsOwnScopeGoes(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("block"), Self.volume("tx")])
        try await broker.mergeRetainedRoots(scope: "chain", roots: ["block", "tx"].map(Self.cid))
        try await broker.mergeRetainedRoots(scope: "chain:body:block", roots: ["block", "tx"].map(Self.cid))

        #expect(try await broker.releaseRetainedRoots(
            scope: "chain:body:block", roots: ["block", "tx"].map(Self.cid)) == 0)
        #expect(await broker.hasVolume(root: Self.cid("block")))
        #expect(await broker.hasVolume(root: Self.cid("tx")))
        #expect(try await broker.retainedScopes(prefix: "chain:body:") == [])
    }

    /// After a crash a node lists its body scopes and drops the ones whose
    /// block it has no record of: the rest, and what they share, stay.
    @Test(arguments: Kind.allCases)
    func leftoverScopesAreFoundByListingAndDropped(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        let blocks = (1...6).map { "block-\($0)" }
        try await broker.storeVolumesLocal(blocks.map { Self.volume($0) } + [Self.volume("shared")])
        for block in blocks {
            try await broker.mergeRetainedRoots(
                scope: "chain:body:\(Self.cid(block))", roots: [Self.cid(block), Self.cid("shared")])
        }
        let recorded = Set(["block-2", "block-5"].map(Self.cid))

        for scope in try await broker.retainedScopes(prefix: "chain:body:") {
            guard !recorded.contains(String(scope.dropFirst("chain:body:".count))) else { continue }
            try await broker.releaseRetainedRoots(scope: scope, roots: try await broker.retainedRoots(scope: scope))
        }

        #expect(try await broker.retainedScopes(prefix: "chain:body:")
            == recorded.map { "chain:body:\($0)" }.sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) })
        for block in blocks {
            #expect(await broker.hasVolume(root: Self.cid(block)) == recorded.contains(Self.cid(block)), "\(block)")
        }
        #expect(await broker.hasVolume(root: Self.cid("shared")))
    }

    /// Thousands of owners beside one scope that retains far more: every
    /// owner is listed once, in order, and releasing some lists the rest.
    @Test(arguments: Kind.allCases)
    func manyScopesAreEachListedOnce(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        let state = (0..<2_000).map { Self.volume("state-\($0)") }
        try await broker.storeVolumesLocal(state + [Self.volume("body")])
        try await broker.advanceRetainedRoots(scope: "chain", roots: state.map(\.root))
        let scopes = (0..<1_500).map { "chain:body:\($0)" }
        for scope in scopes {
            try await broker.mergeRetainedRoots(scope: scope, roots: [Self.cid("body")])
        }
        let ordered = scopes.sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) }
        #expect(try await broker.retainedScopes(prefix: "chain:body:") == ordered)
        #expect(try await broker.retainedScopes(prefix: "chain:body:14") == ordered.filter { $0.hasPrefix("chain:body:14") })
        #expect(try await broker.retainedScopes(prefix: "chain").count == 1_501)

        for scope in scopes where scope.hasSuffix("7") {
            try await broker.releaseRetainedRoots(scope: scope, roots: [Self.cid("body")])
        }
        #expect(try await broker.retainedScopes(prefix: "chain:body:") == ordered.filter { !$0.hasSuffix("7") })
        // Still named by the scopes that are left.
        #expect(await broker.hasVolume(root: Self.cid("body")))
        #expect(try await broker.retainedRoots(scope: "chain").count == 2_000)
    }

    /// Two spellings of one text are two scopes, and a prefix is a prefix
    /// of bytes: Swift's own string equality and `hasPrefix` say otherwise.
    @Test(arguments: Kind.allCases)
    func scopeNamesAreTheirBytes(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("one"), Self.volume("two")])
        let composed = "caf\u{E9}"
        let decomposed = "cafe\u{301}"
        try await broker.advanceRetainedRoots(scope: composed, roots: [Self.cid("one")])
        try await broker.advanceRetainedRoots(scope: decomposed, roots: [Self.cid("two")])

        #expect(try await broker.retainedRoots(scope: composed) == [Self.cid("one")])
        #expect(try await broker.retainedRoots(scope: decomposed) == [Self.cid("two")])
        let all = try await broker.retainedScopes(prefix: "caf")
        #expect(all.map { Array($0.utf8) } == [Array(decomposed.utf8), Array(composed.utf8)])
        let plain = try await broker.retainedScopes(prefix: "cafe")
        #expect(plain.map { Array($0.utf8) } == [Array(decomposed.utf8)])
    }

    @Test(arguments: Kind.allCases)
    func aScopeNameWithANULIsRefused(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.store(volume: Self.volume("kept"))
        await #expect(throws: BrokerError.self) {
            try await broker.advanceRetainedRoots(scope: "a\0b", roots: [Self.cid("kept")])
        }
        await #expect(throws: BrokerError.self) {
            try await broker.mergeRetainedRoots(scope: "a\0b", roots: [Self.cid("kept")])
        }
        await #expect(throws: BrokerError.self) {
            try await broker.releaseRetainedRoots(scope: "a\0b", roots: [Self.cid("kept")])
        }
        #expect(try await broker.retainedScopes(prefix: "") == [])
    }

    @Test(arguments: Kind.allCases)
    func aRootNameWithANULIsRefused(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.store(volume: Self.volume("kept"))
        try await broker.advanceRetainedRoots(scope: "s", roots: [Self.cid("kept")])
        let truncating = Self.cid("kept") + "\0junk"

        await #expect(throws: BrokerError.self) {
            try await broker.releaseRetainedRoots(scope: "s", roots: [truncating])
        }
        await #expect(throws: BrokerError.self) {
            try await broker.mergeRetainedRoots(scope: "s", roots: [truncating])
        }
        #expect(try await broker.retainedRoots(scope: "s") == [Self.cid("kept")])
        #expect(await broker.hasVolume(root: Self.cid("kept")))
    }

    @Test(arguments: Kind.allCases)
    func releasingAVolumeInNoScopeRemovesIt(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.storeVolumesLocal([Self.volume("stray", ["stray-leaf"])])

        #expect(try await broker.releaseRetainedRoots(scope: "a", roots: [Self.cid("stray")]) == 1)
        #expect(await broker.hasVolume(root: Self.cid("stray")) == false)
        #expect(await broker.fetchDataLocal(cid: Self.cid("stray-leaf")) == nil)
    }

    @Test(arguments: Kind.allCases)
    func advancingToAnAbsentRootIsRefusedWithoutChange(kind: Kind) async throws {
        let broker = try Self.broker(kind)
        try await broker.store(volume: Self.volume("kept"))
        try await broker.advanceRetainedRoots(scope: "s", roots: [Self.cid("kept")])
        try await broker.store(volume: Self.volume("swept"))
        try await broker.sweep()

        for absent in [Self.cid("never-stored"), Self.cid("swept")] {
            await #expect(throws: BrokerError.missingRetainedRoot(absent)) {
                try await broker.advanceRetainedRoots(scope: "s", roots: [absent])
            }
            await #expect(throws: BrokerError.missingRetainedRoot(absent)) {
                try await broker.mergeRetainedRoots(scope: "s", roots: [absent])
            }
            #expect(try await broker.retainedRoots(scope: "s") == [Self.cid("kept")])
        }
    }

    @Test func advancingToAnIncompleteRootIsRefusedWithoutChange() async throws {
        let path = NSTemporaryDirectory() + "vb_retention_\(UUID().uuidString).sqlite"
        let broker = try DiskBroker(path: path)
        let kept = Self.cid("kept")
        let incomplete = Self.cid("incomplete")
        try await broker.storeVolumesLocal([Self.volume("kept"), Self.volume("incomplete", ["member"])])
        try await broker.advanceRetainedRoots(scope: "s", roots: [kept])

        let connection = try SQLiteConnection(path: path)
        try await connection.write {
            try connection.exec("DELETE FROM volume_entries WHERE root='\(incomplete)' AND cid='\(Self.cid("member"))'")
        }
        #expect(await broker.hasVolume(root: incomplete) == false)

        await #expect(throws: BrokerError.missingRetainedRoot(incomplete)) {
            try await broker.advanceRetainedRoots(scope: "s", roots: [kept, incomplete])
        }
        await #expect(throws: BrokerError.missingRetainedRoot(incomplete)) {
            try await broker.mergeRetainedRoots(scope: "s", roots: [incomplete])
        }
        #expect(try await broker.retainedRoots(scope: "s") == [kept])
    }

    // MARK: - Memory/Disk equivalence

    /// SplitMix64: a deterministic generator so failures replay by seed.
    private struct SeededGenerator: RandomNumberGenerator {
        var state: UInt64
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
    }

    private struct Observation: Equatable {
        var result: String
        var volumes: [Bool]
        var data: [Data?]
        var scopes: [[String]]
    }

    private static let scopes = ["a", "b"]

    /// A DAG of Volumes: Volume `i` may nest Volumes `j > i`, and leaves are
    /// shared across Volumes.
    private static func universe(_ rng: inout SeededGenerator) -> [SerializedVolume] {
        let count = 10
        return (0..<count).map { i in
            var members = (i + 1..<count).filter { _ in Int.random(in: 0..<4, using: &rng) == 0 }
                .map { "v\($0)" }
            members.append("leaf\(Int.random(in: 0..<5, using: &rng))")
            return volume("v\(i)", members)
        }
    }

    private static func observe(
        _ broker: any RetainedRootReleaseBroker,
        result: String,
        universe: [SerializedVolume],
        cids: [String]
    ) async throws -> Observation {
        var observation = Observation(result: result, volumes: [], data: [], scopes: [])
        for volume in universe {
            observation.volumes.append(await broker.hasVolume(root: volume.root))
        }
        for cid in cids {
            observation.data.append(await broker.fetchDataLocal(cid: cid))
        }
        for scope in scopes {
            observation.scopes.append(try await broker.retainedRoots(scope: scope))
        }
        observation.scopes.append(try await broker.retainedScopes(prefix: ""))
        return observation
    }

    private static func apply(
        _ broker: any RetainedRootReleaseBroker,
        op: Int,
        roots: [String],
        batch: [SerializedVolume],
        scope: String
    ) async -> String {
        do {
            switch op {
            case 0: try await broker.storeVolumesLocal(batch); return "stored"
            case 1: try await broker.advanceRetainedRoots(scope: scope, roots: roots); return "advanced"
            case 2: try await broker.mergeRetainedRoots(scope: scope, roots: roots); return "merged"
            case 3: return "released \(try await broker.releaseRetainedRoots(scope: scope, roots: roots))"
            default: return "swept \(try await broker.sweep())"
            }
        } catch {
            return "error \(error)"
        }
    }

    @Test(arguments: 0..<16)
    func memoryAndDiskAgreeOnRandomSequences(seed: Int) async throws {
        var rng = SeededGenerator(state: UInt64(seed))
        let universe = Self.universe(&rng)
        let cids = Array(Set(universe.flatMap(\.entries.keys))).sorted() + [Self.cid("absent")]
        let memory = try Self.broker(.memory)
        let disk = try Self.broker(.disk)

        // Start with everything stored and each scope retaining some of it,
        // so that sweep and release meet retained roots in every sequence.
        for broker in [memory, disk] {
            try await broker.storeVolumesLocal(universe)
        }
        for scope in Self.scopes {
            let roots = universe.map(\.root).filter { _ in Bool.random(using: &rng) }
            for broker in [memory, disk] {
                try await broker.advanceRetainedRoots(scope: scope, roots: roots)
            }
        }

        for step in 0..<60 {
            let op = Int.random(in: 0..<5, using: &rng)
            let scope = Self.scopes.randomElement(using: &rng)!
            let batch = (0..<Int.random(in: 1...3, using: &rng)).map { _ in
                universe.randomElement(using: &rng)!
            }
            var roots = universe.map(\.root).filter { _ in Bool.random(using: &rng) }
            // Mostly roots that are stored, so that retaining succeeds and
            // sweep and release meet retained roots.
            if Int.random(in: 0..<8, using: &rng) != 0 {
                var stored: [String] = []
                for root in roots where await memory.hasVolume(root: root) { stored.append(root) }
                roots = stored
            }
            if Int.random(in: 0..<8, using: &rng) == 0 { roots.append(Self.cid("absent")) }

            let memoryResult = await Self.apply(memory, op: op, roots: roots, batch: batch, scope: scope)
            let diskResult = await Self.apply(disk, op: op, roots: roots, batch: batch, scope: scope)
            let memoryObserved = try await Self.observe(memory, result: memoryResult, universe: universe, cids: cids)
            let diskObserved = try await Self.observe(disk, result: diskResult, universe: universe, cids: cids)
            #expect(memoryObserved == diskObserved, "seed \(seed) step \(step) op \(op)")
            if memoryObserved != diskObserved { return }
        }
    }
}
