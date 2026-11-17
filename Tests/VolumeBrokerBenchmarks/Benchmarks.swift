import Testing
import Foundation
import CID
import Multihash
@testable import VolumeBroker

@Suite("Benchmarks")
struct Benchmarks {

    private enum BenchmarkError: Error {
        case missingVolume(String)
    }

    // MARK: - Helpers

    private func cid(for data: Data) -> String {
        let multihash = try! Multihash(raw: data, hashedWith: .sha2_256)
        return try! CID(version: .v1, codec: .dag_cbor, multihash: multihash).toBaseEncodedString
    }

    private func cid(_ value: String) -> String {
        cid(for: Data(value.utf8))
    }

    private func payload(_ root: String, entryCount: Int, dataSize: Int = 64) -> SerializedVolume {
        let rootData = Data(root.utf8)
        var entries = [cid(for: rootData): rootData]
        entries.reserveCapacity(entryCount)
        for i in 1..<entryCount {
            var data = Data("\(root):\(i):".utf8)
            if data.count < dataSize {
                data.append(Data(repeating: UInt8(i & 0xFF), count: dataSize - data.count))
            }
            entries[cid(for: data)] = data
        }
        return SerializedVolume(root: cid(for: rootData), entries: entries)
    }

    private func tempDB() throws -> DiskBroker {
        let path = NSTemporaryDirectory() + "vb_bench_\(UUID().uuidString).sqlite"
        return try DiskBroker(path: path)
    }

    private func measure(_ label: String, iterations: Int = 1, _ body: () async throws -> Void) async throws {
        let clock = ContinuousClock()
        let elapsed = try await clock.measure {
            for _ in 0..<iterations {
                try await body()
            }
        }
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        let perOp = ms / Double(iterations)
        print("  [\(label)] \(iterations) iterations in \(String(format: "%.2f", ms))ms (\(String(format: "%.3f", perOp))ms/op)")
    }

    private func processCPUMilliseconds() -> Double {
        var usage = rusage()
        getrusage(RUSAGE_SELF, &usage)
        func ms(_ t: timeval) -> Double { Double(t.tv_sec) * 1000 + Double(t.tv_usec) / 1000 }
        return ms(usage.ru_utime) + ms(usage.ru_stime)
    }

    /// Wall time, and the process's CPU time, per operation: on a shared
    /// machine the CPU time is the steadier of the two.
    private func measureCPU(_ label: String, iterations: Int, _ body: () async throws -> Void) async throws {
        let clock = ContinuousClock()
        let cpuStart = processCPUMilliseconds()
        let elapsed = try await clock.measure {
            for _ in 0..<iterations { try await body() }
        }
        let cpu = processCPUMilliseconds() - cpuStart
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        print("  [\(label)] \(iterations) iterations: wall \(String(format: "%.3f", ms / Double(iterations)))ms/op, cpu \(String(format: "%.3f", cpu / Double(iterations)))ms/op")
    }

    // MARK: - DiskBroker Store

    @Test func diskStoreSmallVolumes() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Store small volumes (10 entries each) ---")
        try await measure("store 1000 volumes", iterations: 1000) {
            let p = payload("r-\(Int.random(in: 0..<1_000_000))", entryCount: 10)
            try await broker.store(volume: p)
        }
    }

    @Test func diskStoreLargeVolume() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Store large volume (1000 entries, 256B each) ---")
        try await measure("store 1 large volume", iterations: 10) {
            let p = payload("large-\(Int.random(in: 0..<1_000_000))", entryCount: 1000, dataSize: 256)
            try await broker.store(volume: p)
        }
    }

    @Test func diskBatchStore() async throws {
        let broker = try tempDB()
        print("\n--- DiskBroker: Batch store (50 volumes × 10 entries in 1 txn) ---")
        try await measure("batch store 50 volumes", iterations: 10) {
            let payloads = (0..<50).map { i in
                payload("batch-\(Int.random(in: 0..<1_000_000))-\(i)", entryCount: 10)
            }
            try await broker.storeVolumesLocal(payloads)
        }
    }

    @Test func diskBatchVsIndividualStore() async throws {
        let count = 50
        print("\n--- DiskBroker: Batch vs Individual store (\(count) volumes × 10 entries) ---")

        let brokerIndividual = try tempDB()
        let individualPayloads = (0..<count).map { i in
            payload("ind-\(i)", entryCount: 10)
        }
        try await measure("individual stores", iterations: 1) {
            for p in individualPayloads {
                try await brokerIndividual.store(volume: p)
            }
        }

        let brokerBatch = try tempDB()
        let batchPayloads = (0..<count).map { i in
            payload("bat-\(i)", entryCount: 10)
        }
        try await measure("batch store", iterations: 1) {
            try await brokerBatch.storeVolumesLocal(batchPayloads)
        }
    }

    /// The write path a node drives per block: a batch of Volumes stored
    /// and retained in one transaction, a retain of stored roots, and a
    /// counted-root adjust. Payloads are built before timing, and taken on
    /// trust, so the time is the broker's.
    @Test func diskWritePathPerOperation() async throws {
        let broker = try tempDB()
        let batches = 200
        let perBatch = 8
        let volumes = (0..<batches).map { b in
            (0..<perBatch).map { v in
                try! ValidatedVolume(trusting: payload("wp-\(b)-\(v)", entryCount: 16, dataSize: 128))
            }
        }
        print("\n--- DiskBroker: write path per operation ---")
        var batch = 0
        try await measureCPU("store+retain batch (\(perBatch) Volumes x 16 entries)", iterations: batches) {
            let roots = volumes[batch].map(\.volume.root)
            try await broker.storeVolumesLocal(volumes[batch], retaining: ["blocks": roots])
            batch += 1
        }
        let roots = volumes.flatMap { $0.map(\.volume.root) }
        var next = 0
        try await measureCPU("retain 1 stored root", iterations: 1_000) {
            try await broker.mergeRetainedRoots(scope: "tips", roots: [roots[next % roots.count]])
            next += 1
        }
        var marker: String?
        var step = 0
        try await measureCPU("adjust 8 roots (0<->1)", iterations: 1_000) {
            let delta = step % 2 == 0 ? 1 : -1
            let window = (0..<8).map { roots[(step / 2 * 8 + $0) % roots.count] }
            let deltas = Dictionary(uniqueKeysWithValues: window.map { ($0, delta) })
            let newMarker = "m\(step)"
            try await broker.adjustCountedRoots(
                scope: "state", deltas: deltas, marker: newMarker, expectedMarker: marker)
            marker = newMarker
            step += 1
        }
    }

    // MARK: - DiskBroker Fetch

    @Test func diskFetch() async throws {
        let broker = try tempDB()
        for i in 0..<100 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Fetch volumes (20 entries each) ---")
        var index = 0
        try await measure("fetch 1000 times", iterations: 1000) {
            let root = cid("r-\(index % 100)")
            index += 1
            guard await broker.fetchVolumeLocal(root: root) != nil else {
                throw BenchmarkError.missingVolume(root)
            }
        }
    }

    @Test func diskSparseFrontierBatchRead() async throws {
        let broker = try tempDB()
        let volumes = (0..<64).map { payload("sparse-\($0)", entryCount: 16) }
        try await broker.storeVolumesLocal(volumes)
        let requested = Set(volumes.flatMap { $0.entries.keys.prefix(2) })
        print("\n--- DiskBroker: sparse Cashew frontier across 64 Volumes ---")

        let found = await broker.fetchDataLocal(cids: requested)
        #expect(found.count == requested.count)
        try await measure("batch \(requested.count) sparse CIDs", iterations: 20) {
            guard await broker.fetchDataLocal(cids: requested).count == requested.count else {
                throw BenchmarkError.missingVolume("sparse frontier")
            }
        }
    }

    @Test func diskHasVolume() async throws {
        let broker = try tempDB()
        for i in 0..<100 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 5))
        }
        print("\n--- DiskBroker: hasVolume checks ---")
        var index = 0
        var hits = 0
        try await measure("hasVolume 10000 checks", iterations: 10000) {
            if await broker.hasVolume(root: cid("r-\(index % 200)")) { hits += 1 }
            index += 1
        }
        #expect(hits == 5_000)
    }

    @Test func largeVolumePresenceAndPointReads() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("VolumeBrokerPointRead-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let connection = try SQLiteConnection(path: directory.appendingPathComponent("volumes.sqlite").path)
        let store = CASVolumeStore(connection: connection)
        let volume = payload("point-read-scale", entryCount: 2_000, dataSize: 128)
        let members = volume.entries.keys.filter { $0 != volume.root }.sorted()
        let target = try #require(members.first)
        let corruptSibling = try #require(members.last)
        let expected = try #require(volume.entries[target])
        try await store.store(volume: volume)
        try await connection.write {
            try connection.exec("UPDATE cas_data SET data=X'00' WHERE cid='\(corruptSibling)'")
        }

        try await measure("large-volume structural presence", iterations: 100) {
            guard await store.hasVolume(root: volume.root) else {
                throw BenchmarkError.missingVolume(volume.root)
            }
        }
        try await measure("large-volume point read", iterations: 1_000) {
            guard await store.fetchDataLocal(cid: target) == expected else {
                throw BenchmarkError.missingVolume(target)
            }
        }

        #expect(await store.hasVolume(root: volume.root))
        #expect(await store.fetchDataLocal(cid: target) == expected)
        #expect(await store.fetchDataLocal(cid: corruptSibling) == Data([0]))
    }

    // MARK: - Sweep

    @Test func diskSweep() async throws {
        print("\n--- DiskBroker: Sweep (500 volumes, 50 retained) ---")
        let broker = try tempDB()
        for i in 0..<500 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 10))
        }
        try await broker.advanceRetainedRoots(scope: "keeper", roots: (0..<50).map { cid("r-\($0)") })
        try await measure("sweep 450 unretained volumes", iterations: 1) {
            let swept = try await broker.sweep()
            #expect(swept == 450)
        }
        #expect(await broker.hasVolume(root: cid("r-0")))
        #expect(await broker.hasVolume(root: cid("r-499")) == false)
    }

    @Test func memorySweep() async throws {
        let broker = MemoryBroker()
        for i in 0..<1000 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 5))
        }
        try await broker.advanceRetainedRoots(scope: "keeper", roots: (0..<100).map { cid("r-\($0)") })
        print("\n--- MemoryBroker: Sweep 900 of 1000 volumes ---")
        try await measure("sweep", iterations: 1) {
            let swept = try await broker.sweep()
            #expect(swept == 900)
        }
    }

    @Test func memoryBatchStore() async throws {
        let broker = MemoryBroker()
        print("\n--- MemoryBroker: Batch store (100 volumes × 10 entries) ---")
        try await measure("batch store", iterations: 10) {
            let payloads = (0..<100).map { i in
                payload("batch-\(Int.random(in: 0..<1_000_000))-\(i)", entryCount: 10)
            }
            try await broker.storeVolumesLocal(payloads)
        }
    }

    // MARK: - Concurrent Reads

    @Test func diskConcurrentReads() async throws {
        let broker = try tempDB()
        for i in 0..<200 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Concurrent reads (4 tasks × 500 fetches) ---")

        let clock = ContinuousClock()
        let start = clock.now
        let successfulReads = await withTaskGroup(of: Int.self) { group in
            for taskIndex in 0..<4 {
                group.addTask {
                    var hits = 0
                    for iteration in 0..<500 {
                        let index = (taskIndex * 500 + iteration) % 200
                        if await broker.fetchVolumeLocal(root: cid("r-\(index)")) != nil { hits += 1 }
                    }
                    return hits
                }
            }
            var total = 0
            for await hits in group { total += hits }
            return total
        }
        let elapsed = start.duration(to: clock.now)
        #expect(successfulReads == 2_000)
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        let opsPerSec = 2000.0 / (ms / 1000.0)
        print("  [4-way concurrent fetch] 2000 total fetches in \(String(format: "%.2f", ms))ms (\(String(format: "%.0f", opsPerSec)) ops/sec)")
    }

    @Test func diskConcurrentReadsWhileWriting() async throws {
        let broker = try tempDB()
        for i in 0..<200 {
            try await broker.store(volume: payload("r-\(i)", entryCount: 20))
        }
        print("\n--- DiskBroker: Concurrent reads + writes (3 readers + 1 writer) ---")

        let clock = ContinuousClock()
        let start = clock.now
        let successfulReads = try await withThrowingTaskGroup(of: Int.self) { group in
            for taskIndex in 0..<3 {
                group.addTask {
                    var hits = 0
                    for iteration in 0..<500 {
                        let index = (taskIndex * 500 + iteration) % 200
                        if await broker.fetchVolumeLocal(root: cid("r-\(index)")) != nil { hits += 1 }
                    }
                    return hits
                }
            }
            group.addTask {
                for i in 200..<400 {
                    try await broker.store(volume: payload("w-\(i)", entryCount: 2))
                }
                return 0
            }
            var total = 0
            for try await hits in group { total += hits }
            return total
        }
        let elapsed = start.duration(to: clock.now)
        #expect(successfulReads == 1_500)
        var committedWrites = 0
        for i in 200..<400 {
            if await broker.hasVolume(root: cid("w-\(i)")) { committedWrites += 1 }
        }
        #expect(committedWrites == 200)
        let ms = Double(elapsed.components.attoseconds) / 1e15 + Double(elapsed.components.seconds) * 1000
        print("  [3 readers + 1 writer] completed in \(String(format: "%.2f", ms))ms")
    }

    // MARK: - Cascade Fetch

    @Test func cascadeFetchPerformance() async throws {
        let disk = try tempDB()
        let memory = MemoryBroker(near: disk)

        for i in 0..<200 {
            try await disk.store(volume: payload("r-\(i)", entryCount: 10))
        }
        for i in 0..<50 {
            try await memory.store(volume: payload("r-\(i)", entryCount: 10))
        }

        print("\n--- Cascade: memory(50) → disk(200) fetch ---")
        var index = 0
        try await measure("fetch 1000 (mix hit/miss)", iterations: 1000) {
            let root = cid("r-\(index % 200)")
            index += 1
            guard await memory.fetchVolume(root: root) != nil else {
                throw BenchmarkError.missingVolume(root)
            }
        }
    }
}
