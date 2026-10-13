# VolumeBroker

Atomic, Volume-granular content-addressed storage for
[Cashew](https://github.com/adalinxx/cashew).

A `SerializedVolume` is one root CID plus the exact `(CID, bytes)` entries
published with it. VolumeBroker stores that set as one unit. It never walks DAG
links; the only relationship it knows is Volume membership. It knows nothing
about chains.

## Contract

- A Volume is visible only after its complete, CID-valid entry set commits.
- Valid CID bytes and Volume membership are immutable.
- Reads may fall through `local -> near -> far`; writes target one broker.
- A retained root keeps its own Volume and nothing nested under it; `sweep`
  removes every Volume no scope retains.
- Equal CIDs are deduplicated within each broker tier.

One cascade is one storage domain. Use separate brokers or database paths when
data must be isolated.

## Usage

```swift
import VolumeBroker

let disk = try DiskBroker(path: "/var/lib/my-app/volumes.sqlite")
let memory = MemoryBroker(near: disk)
```

Reads now try memory and then disk. Stores remain explicit.

Let Cashew produce valid Volume boundaries:

```swift
// Store the root and one selected nested Volume.
try await root.store(
    paths: [["accounts", "alice"]: .targeted],
    storer: disk
)
```

`.recursive` selects every nested Volume below a path. Each emitted Volume is
still an independent storage and retention unit. Cashew submits one Volume per
storer callback; callers that already hold an all-or-none batch can use
`storeVolumesLocal` to commit it in one transaction.

Resolve through the read cascade:

```swift
let resolved = try await unresolvedRoot.resolveRecursive(source: memory)
```

`VolumeBroker.fetch(_:)` resolves each Cashew frontier with one batched read per
storage tier.

## Retention

A scope is a name; a retained root is a complete stored Volume root. Advancing
a scope atomically replaces its root set and refuses any root that is not a
complete stored Volume, so a retained root never names content the broker lacks:

```swift
try await disk.advanceRetainedRoots(scope: "canonical", roots: [stateRoot])
```

`mergeRetainedRoots` adds roots without replacing the set. Replacing a scope
with the same set and merging roots already in it are naturally idempotent.

A Volume is kept if a scope retains its root, and only then. Retaining a root
does not keep a Volume nested under it: a caller that wants one kept retains
its root too.

```swift
let removed = try await disk.sweep()
```

`sweep` removes every Volume no scope retains, then every CAS row no surviving
Volume owns, in one transaction. It has no grace window: content stored but not
yet retained is swept, and a later advance naming it is refused. Callers store,
then advance, and sweep only between their own commits.

```swift
let removed = try await disk.releaseRetainedRoots(scope: "canonical", roots: [oldRoot])
```

`releaseRetainedRoots` takes roots out of a scope and removes those of them no
scope retains, with the CAS rows that leaves unowned. Its work is in the roots
named, not in the size of the store, and it touches nothing else.

## Implementations

- `MemoryBroker` is the in-memory twin with identical retention semantics.
- `DiskBroker` uses SQLite, WAL, foreign keys, and schema v2 validation. With
  `synchronous=FULL`, each commit is fsynced to the WAL before it returns, so a
  `store` that returns survives a crash.
- Every broker is directly usable as Cashew's `VolumeStorer`, `ContentSource`,
  and `Fetcher`.

`DiskBroker` initializes only an empty v0 database. Any other version, including
v1 (owner/count pins), fails closed with `migrationRequired`; rebuild from a new
database path. Malformed v2 schemas fail closed.

## Boundary

| VolumeBroker owns | The caller owns |
| --- | --- |
| Volume validation and atomic publication | DAG traversal and Volume selection |
| Local CAS, read cascading, and the sweep | Storage placement and domain isolation |
| Retained-root sets | Which roots are retained, and when to sweep |
| Storage integrity | Application state, canonicity, and consensus |

See [correctness invariants](docs/correctness-invariants.md) for the review
contract.

## Requirements

- Swift 6.0+
- macOS 13+ or iOS 16+
- Cashew 4.0.1

```sh
swift build
swift test
```
