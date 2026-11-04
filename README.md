# VolumeBroker

Atomic, Volume-granular content-addressed storage for
[Cashew](https://github.com/0xLattice/cashew).

A `SerializedVolume` is one root CID plus the exact `(CID, bytes)` entries
published with it. VolumeBroker stores that set as one unit. It never walks DAG
links; the only relationship it knows is Volume membership. It knows nothing
about chains.

## Contract

- A Volume is visible only after its complete, CID-valid entry set commits.
- Valid CID bytes and Volume membership are immutable.
- Reads may fall through `local -> near -> far`; writes target one broker.
- A retained or counted root keeps its own Volume and nothing nested under it;
  `sweep` removes every Volume no scope retains or counts.
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

`retainedScopes(prefix:)` lists the scopes that retain something. A caller
that keeps one scope per thing it holds uses it to find the ones it no longer
has a reason for.

- A scope's name is its bytes. A prefix is a prefix of bytes, the listing is
  in byte order, and a name with a NUL is refused.
- A listing is one snapshot of committed state, as every read is: reads run
  on a connection of their own.
- A scope is let go of with `releaseRetainedRoots`, naming what it retains.
  Emptying one with `advanceRetainedRoots(scope:roots: [])` removes it from
  the listing and reclaims nothing: its Volumes stay until a sweep, or a
  release that names them.
- Storing and then retaining are two commits. A release or a sweep between
  them removes what was stored, and the retain is then refused whole with
  `missingRetainedRoot`: store again and retry.

### Counted roots

A `CountedRootBroker` also keeps, per scope, a count for each root and one
marker, and changes them together:

```swift
try await disk.adjustCountedRoots(
    scope: "state", deltas: [newNode: 1, oldNode: -1],
    marker: "block-12", expectedMarker: "block-11")
```

The adjustment is one transaction and is refused whole, with nothing changed,
when the scope's marker is not `expectedMarker` (`nil` for none,
`countedMarkerMismatch`), when a count would go below zero (`negativeCount`),
or when a root would start being counted and is not a complete stored Volume
(`countedRootsMissing`, listing every such root). Zero deltas are ignored. A
root whose count reaches zero leaves the scope and, if no scope counts or
retains it, is removed with the CAS rows that leaves unowned. The marker lets
a caller keep its own cursor in the same commit as its counts.

A Volume is kept while any scope counts it or any scope retains it: release,
sweep, adjustment and drop all check both. Counted scopes are a namespace of
their own. `countedRoots(scope:)` and `countedMarker(scope:)` read one scope,
`countedScopes(prefix:)` lists the scopes with a marker by the same byte rules
as `retainedScopes(prefix:)`, and `dropCountedScope(scope:)` deletes a scope's
counts and marker and reclaims what nothing else keeps.

- The counted tables hold no foreign key, so a binary that predates them still
  opens the store. That binary's release and sweep do not see counts and may
  remove a counted Volume. On the next open, a scope that counts a root no
  longer stored loses its counts and its marker, and its owner starts it again.

## Implementations

- `MemoryBroker` is the in-memory twin with identical retention and counting
  semantics.
- `DiskBroker` uses SQLite, WAL, foreign keys, and schema v2 validation. With
  `synchronous=FULL`, each commit is fsynced to the WAL before it returns, so a
  `store` that returns survives a crash.
- Every broker is directly usable as Cashew's `VolumeStorer`, `ContentSource`,
  and `Fetcher`.

`DiskBroker` initializes only an empty v0 database. Any other version, including
v1 (owner/count pins), fails closed with `migrationRequired`; rebuild from a new
database path. Malformed v2 schemas fail closed. A v2 store without the counted
tables gets them on open, at the same version.

## Boundary

| VolumeBroker owns | The caller owns |
| --- | --- |
| Volume validation and atomic publication | DAG traversal and Volume selection |
| Local CAS, read cascading, and the sweep | Storage placement and domain isolation |
| Retained-root sets and counted roots | Which roots are retained or counted, and when to sweep |
| Storage integrity | Application state, canonicity, and consensus |

See [correctness invariants](docs/correctness-invariants.md) for the review
contract. This README and the correctness invariants are published at
[lattice.build/docs](https://lattice.build/docs/protocol/volumebroker/).

## Requirements

- Swift 6.0+
- macOS 13+ or iOS 16+
- Cashew 4.0.1

```sh
swift build
swift test
```
