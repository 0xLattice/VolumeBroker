import Foundation
import XCTest
#if canImport(SQLite3)
import SQLite3
#else
import VolumeBrokerSQLite
#endif
#if canImport(Darwin)
import Darwin
#endif
@testable import VolumeBroker

nonisolated(unsafe) private var casDeletesPrepared: Int32 = 0

/// Re-review probes: a process that dies inside a release, or anywhere in a
/// run of body fetches and releases, and what the next process finds.
/// Same self-re-executing child as `VolumeIntegrityTests`.
final class ScopeCrashTests: XCTestCase {
#if os(macOS)
    private static let childPathKey = "VOLUME_BROKER_SCOPE_CRASH_CHILD_PATH"
    private typealias P = ScopeTests

    private func sanitizerIsActive() -> Bool {
        guard let handle = dlopen(nil, RTLD_NOW) else { return false }
        defer { dlclose(handle) }
        return dlsym(handle, "__asan_init") != nil || dlsym(handle, "__tsan_init") != nil
    }

    private func temporaryPath() throws -> (directory: URL, path: String) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("ScopeCrash-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return (directory, directory.appendingPathComponent("volumes.sqlite").path)
    }

    private func spawnChild(testName: String, path: String) throws -> (Process, URL) {
        let outputURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("ScopeCrashChild-\(UUID().uuidString).log")
        try Data().write(to: outputURL)
        let outputHandle = try FileHandle(forWritingTo: outputURL)
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/xcrun")
        process.arguments = [
            "xctest", "-XCTest", "VolumeBrokerTests.ScopeCrashTests/\(testName)",
            Bundle(for: ScopeCrashTests.self).bundlePath,
        ]
        process.environment = ProcessInfo.processInfo.environment
            .merging([Self.childPathKey: path]) { _, child in child }
        process.standardOutput = outputHandle
        process.standardError = outputHandle
        try process.run()
        return (process, outputURL)
    }

    private func wait(_ process: Process, seconds: Double) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(seconds))
        while process.isRunning && ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    private static let chain = "bafychain:Nexus"
    private static func body(_ label: String) -> String { "\(chain):body:\(P.cid(label))" }

    /// The process exits while a release's transaction is open: retained
    /// rows deleted, one Volume and some of its CAS rows deleted, no COMMIT.
    func testAProcessThatDiesInsideAReleaseLeavesTheScopeWhole() async throws {
        if sanitizerIsActive() { throw XCTSkip("instrumented xctest bundles cannot be re-launched through xcrun") }
        let universe = [
            P.volume("state", ["state-leaf"]),
            P.volume("block-a", ["tx-1", "a-leaf"]), P.volume("tx-1", ["leaf-1", "both-leaf"]),
            P.volume("tx-2", ["leaf-2", "both-leaf"]), P.volume("shared-tx", ["shared-leaf"]),
            P.volume("block-b", ["shared-tx"]),
        ]
        let a = ["block-a", "tx-1", "tx-2", "shared-tx"].map(P.cid).sorted()

        if let childPath = ProcessInfo.processInfo.environment[Self.childPathKey] {
            let connection = try SQLiteConnection(path: childPath)
            let store = CASVolumeStore(connection: connection)
            let retained = RetainedRootIndex(connection: connection)
            try await store.storeVolumesLocal(universe)
            try await retained.mergeRetainedRoots(scope: Self.chain, roots: [P.cid("state")])
            try await retained.mergeRetainedRoots(scope: Self.body("block-a"), roots: a)
            try await retained.mergeRetainedRoots(scope: Self.body("block-b"), roots: ["block-b", "shared-tx"].map(P.cid))
            // The authorizer runs as each statement is prepared, and a release
            // prepares its statements one at a time as it goes.
            sqlite3_set_authorizer(connection.db, { _, action, table, _, _, _ in
                guard action == SQLITE_DELETE, let table, String(cString: table) == "cas_data" else { return SQLITE_OK }
                casDeletesPrepared += 1
                if casDeletesPrepared == 4 { Darwin._exit(0) }
                return SQLITE_OK
            }, nil)
            _ = try await retained.releaseRetainedRoots(scope: Self.body("block-a"), roots: a)
            Darwin._exit(3)
        }

        let location = try temporaryPath()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let (child, log) = try spawnChild(testName: "testAProcessThatDiesInsideAReleaseLeavesTheScopeWhole", path: location.path)
        defer { try? FileManager.default.removeItem(at: log) }
        try await wait(child, seconds: 30)
        XCTAssertFalse(child.isRunning)
        XCTAssertEqual(child.terminationStatus, 0, String(decoding: (try? Data(contentsOf: log)) ?? Data(), as: UTF8.self))

        let reopened = try DiskBroker(path: location.path)
        let listed = try await reopened.retainedScopes(prefix: "\(Self.chain):body:")
        XCTAssertEqual(Set(listed), [Self.body("block-a"), Self.body("block-b")])
        let stillRetained = try await reopened.retainedRoots(scope: Self.body("block-a"))
        XCTAssertEqual(stillRetained, a)
        let before = try await P.apiProblems(reopened, universe: universe)
        XCTAssertEqual(before, [])
        XCTAssertEqual(P.diskProblems(location.path), [])

        // What the node then does: it has no record of block-a.
        let removed = try await reopened.releaseRetainedRoots(scope: Self.body("block-a"), roots: stillRetained)
        XCTAssertEqual(removed, 3)
        let after = try await P.apiProblems(reopened, universe: universe)
        XCTAssertEqual(after, [])
        let shared = await reopened.hasVolume(root: P.cid("shared-tx"))
        XCTAssertTrue(shared)
        XCTAssertEqual(P.diskProblems(location.path), [])
    }

    /// The process exits while a store replacing a retained root's entry list
    /// is reclaiming the dropped padding: entries already swapped, no COMMIT.
    func testAProcessThatDiesInsideAReplaceLeavesTheOldListWhole() async throws {
        if sanitizerIsActive() { throw XCTSkip("instrumented xctest bundles cannot be re-launched through xcrun") }
        let padded = P.volume("block", ["tx-a", "tx-b", "pad-1", "pad-2"])
        let correct = P.volume("block", ["tx-a", "tx-b"])

        if let childPath = ProcessInfo.processInfo.environment[Self.childPathKey] {
            let connection = try SQLiteConnection(path: childPath)
            let store = CASVolumeStore(connection: connection)
            let retained = RetainedRootIndex(connection: connection)
            try await store.store(volume: padded)
            try await retained.mergeRetainedRoots(scope: Self.chain, roots: [padded.root])
            sqlite3_set_authorizer(connection.db, { _, action, table, _, _, _ in
                guard action == SQLITE_DELETE, let table, String(cString: table) == "cas_data" else { return SQLITE_OK }
                Darwin._exit(0)
            }, nil)
            try await store.store(volume: correct)
            Darwin._exit(3)
        }

        let location = try temporaryPath()
        defer { try? FileManager.default.removeItem(at: location.directory) }
        let (child, log) = try spawnChild(testName: "testAProcessThatDiesInsideAReplaceLeavesTheOldListWhole", path: location.path)
        defer { try? FileManager.default.removeItem(at: log) }
        try await wait(child, seconds: 30)
        XCTAssertFalse(child.isRunning)
        XCTAssertEqual(child.terminationStatus, 0, String(decoding: (try? Data(contentsOf: log)) ?? Data(), as: UTF8.self))

        let reopened = try DiskBroker(path: location.path)
        let before = await reopened.fetchVolumeLocal(root: padded.root)
        XCTAssertEqual(before?.entries, padded.entries)
        XCTAssertEqual(P.diskProblems(location.path), [])
        XCTAssertEqual(P.tableCounts(location.path), [1, 5, 5, 1])

        try await reopened.store(volume: correct)
        let after = await reopened.fetchVolumeLocal(root: correct.root)
        XCTAssertEqual(after?.entries, correct.entries)
        let retainedRoots = try await reopened.retainedRoots(scope: Self.chain)
        XCTAssertEqual(retainedRoots, [correct.root])
        XCTAssertEqual(P.diskProblems(location.path), [])
        XCTAssertEqual(P.tableCounts(location.path), [1, 3, 3, 1])
    }

    /// Killed at an arbitrary moment in a run of store, merge, release.
    func testAProcessKilledAmongFetchesAndReleasesLeavesOnlyWhatAListingOrASweepFinds() async throws {
        if sanitizerIsActive() { throw XCTSkip("instrumented xctest bundles cannot be re-launched through xcrun") }
        func volumes(_ i: Int) -> [SerializedVolume] {
            [P.volume("block-\(i)", ["shared-tx", "block-\(i)-leaf"]),
             P.volume("tx-\(i)", (0..<20).map { "tx-\(i)-leaf-\($0)" } + ["every-leaf"]),
             P.volume("shared-tx", ["shared-leaf"])]
        }
        if let childPath = ProcessInfo.processInfo.environment[Self.childPathKey] {
            let broker = try DiskBroker(path: childPath)
            try await broker.storeVolumesLocal([P.volume("state")])
            try await broker.mergeRetainedRoots(scope: Self.chain, roots: [P.cid("state")])
            var i = 0
            while true {
                let batch = volumes(i)
                try await broker.storeVolumesLocal(batch)
                try await broker.mergeRetainedRoots(scope: Self.body("block-\(i)"), roots: batch.map(\.root))
                // Keeps the last three bodies.
                if i >= 3 {
                    let scope = Self.body("block-\(i - 3)")
                    try await broker.releaseRetainedRoots(scope: scope, roots: try await broker.retainedRoots(scope: scope))
                }
                if i == 30 { FileManager.default.createFile(atPath: childPath + ".started", contents: nil) }
                i += 1
            }
        }

        for round in 0..<6 {
            let location = try temporaryPath()
            defer { try? FileManager.default.removeItem(at: location.directory) }
            let (child, log) = try spawnChild(
                testName: "testAProcessKilledAmongFetchesAndReleasesLeavesOnlyWhatAListingOrASweepFinds", path: location.path)
            defer { try? FileManager.default.removeItem(at: log) }
            let deadline = ContinuousClock.now.advanced(by: .seconds(30))
            while !FileManager.default.fileExists(atPath: location.path + ".started"), ContinuousClock.now < deadline {
                try await Task.sleep(for: .milliseconds(5))
            }
            XCTAssertTrue(child.isRunning, String(decoding: (try? Data(contentsOf: log)) ?? Data(), as: UTF8.self))
            try await Task.sleep(for: .microseconds(Int.random(in: 0...60_000)))
            Darwin.kill(child.processIdentifier, SIGKILL)
            try await wait(child, seconds: 5)

            let reopened = try DiskBroker(path: location.path)
            XCTAssertEqual(P.diskProblems(location.path, allowUnretained: true), [], "round \(round)")
            let listed = try await reopened.retainedScopes(prefix: "\(Self.chain):body:")
            XCTAssertTrue((2...4).contains(listed.count), "round \(round): \(listed.count) body scopes")
            for scope in listed {
                let roots = try await reopened.retainedRoots(scope: scope)
                XCTAssertEqual(roots.count, 3, "round \(round): a merge is all or none")
            }
            // A store whose merge never ran is in no scope: the listing cannot find it.
            let unnamed = P.count(location.path, "SELECT COUNT(*) FROM volume_metadata WHERE root NOT IN (SELECT root FROM retained_roots)")
            XCTAssertTrue((0...2).contains(unnamed), "round \(round): \(unnamed) stored Volumes in no scope")

            // The node has no record of any of these blocks.
            for scope in listed {
                try await reopened.releaseRetainedRoots(scope: scope, roots: try await reopened.retainedRoots(scope: scope))
            }
            let afterCleanup = try await reopened.retainedScopes(prefix: "")
            XCTAssertEqual(afterCleanup, [Self.chain])
            let left = P.count(location.path, "SELECT COUNT(*) FROM volume_metadata") - 1
            XCTAssertEqual(left, unnamed, "round \(round): cleanup by listing leaves exactly the never-merged store")
            let swept = try await reopened.sweep()
            XCTAssertEqual(swept, unnamed)
            XCTAssertEqual(P.diskProblems(location.path), [])
            XCTAssertEqual(P.tableCounts(location.path), [1, 1, 1, 1])
        }
    }
#endif
}
