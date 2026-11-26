# VolumeBroker

VolumeBroker is a Swift library that stores content-addressed data one
[Volume](https://lattice.build/docs/protocol/cashew/) at a time: a root CID
and the exact `(CID, bytes)` entries published with it, committed as one
unit. In Lattice it is the node's content store. The node keeps one
VolumeBroker database per hosted chain, holding block bodies, state and the
chain's rules, and uses retention to decide what stays on disk.

VolumeBroker never walks DAG links. The only relationship it knows is which
entries belong to which Volume. It knows nothing about chains.

## Quick example

```swift
import VolumeBroker

let disk = try DiskBroker(path: "/var/lib/my-app/volumes.sqlite")
let memory = MemoryBroker(near: disk)

// Store: cashew produces the Volume boundaries, the broker commits each one.
try await root.store(
    paths: [["accounts", "alice"]: .targeted],
    storer: disk
)

// Retain: a stored Volume is kept only while a scope names its root.
try await disk.advanceRetainedRoots(scope: "canonical", roots: [root.rawCID])

// Read: memory first, then disk.
let resolved = try await unresolvedRoot.resolveRecursive(source: memory)

// Reclaim: remove every Volume no scope retains or counts.
let removed = try await disk.sweep()
```

Store, then retain, then sweep. A sweep between a store and its retain
removes what was stored. `storeVolumesLocal(_:retaining:)` stores and retains
in one transaction.

## Key concepts

| Concept | What it is |
| --- | --- |
| Volume | A root CID plus the entries published with it, defined by [cashew](https://lattice.build/docs/protocol/cashew/). The unit VolumeBroker stores, reads and removes. |
| Broker | One store. `DiskBroker` is SQLite; `MemoryBroker` is its in-memory twin with identical retention and counting semantics. |
| Read cascade | A broker may link a `near` and a `far` broker. A read falls through `local -> near -> far` for what a tier lacks. Writes target one broker. |
| Retained root | A stored Volume root that a named scope keeps. Advancing a scope replaces its root set. |
| Counted root | A root with a count in a scope, changed together with the scope's marker in one transaction. |
| Sweep | Removes every Volume no scope retains or counts, and the bytes that leaves unowned. |

Retaining or counting a root keeps that Volume and nothing nested under it. A
caller that wants a nested Volume kept retains its root too.

Every broker is directly usable as cashew's `VolumeStorer`, `ContentSource`
and `Fetcher`.

By default a `DiskBroker` store that returns survives a crash.
[VolumeBroker architecture](docs/architecture.md#durability) describes the
deferred mode.

## Boundary

| VolumeBroker owns | The caller owns |
| --- | --- |
| Volume validation and atomic publication | DAG traversal and Volume selection |
| Local CAS, read cascading and the sweep | Storage placement and domain isolation |
| Retained-root sets and counted roots | Which roots are retained or counted, and when to sweep |
| Storage integrity | Application state, canonicity and consensus |

One cascade is one storage domain. Use separate brokers or database paths
when data must be isolated.

## Documentation

Published at
[lattice.build/docs](https://lattice.build/docs/protocol/volumebroker/) from
the files below.

| Page | What it covers |
| --- | --- |
| [VolumeBroker architecture](docs/architecture.md) | The storage contract, storing and reading, retention, counted roots, durability and the SQLite schema. |
| [VolumeBroker invariants](docs/correctness-invariants.md) | The rules a change to VolumeBroker must keep, each with its ID and the code and tests that enforce it. |

## Install

```swift
.package(url: "https://github.com/0xLattice/VolumeBroker.git", from: "10.4.1")
```

| Requirement | Version |
| --- | --- |
| Swift | 6.0+ |
| Platforms | macOS 13+ or iOS 16+ |
| cashew | 5.0.7 (pinned exactly in `Package.swift`) |

```sh
swift build
swift test
```
