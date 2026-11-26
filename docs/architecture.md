# VolumeBroker architecture

How VolumeBroker stores, reads, retains and removes Volumes. For what the
library is and a first example, see the [README](../README.md). A Volume is
defined by [cashew](https://lattice.build/docs/protocol/cashew/).

A `SerializedVolume` is one root CID plus the exact `(CID, bytes)` entries
published with it. VolumeBroker stores that set as one unit. It never walks
DAG links; the only relationship it knows is Volume membership.

## Contract

- A Volume is visible only after its complete entry set commits, CID-valid as
  checked on its way in.
- Bytes are hashed once, on their way in. Reads return the bytes as stored.
- Valid CID bytes and Volume membership are immutable.
- Reads may fall through `local -> near -> far` for what a tier lacks. A
  tier's damaged bytes are served, not fallen through. Writes target one
  broker.
- A retained or counted root keeps its own Volume and nothing nested under
  it. `sweep` removes every Volume no scope retains or counts.
- Equal CIDs are deduplicated within each broker tier.

One cascade is one storage domain. Use separate brokers or database paths
when data must be isolated.

[VolumeBroker invariants](correctness-invariants.md) states these as rules
with IDs.

## Storing

| Input | What the broker checks |
| --- | --- |
| `SerializedVolume` | Structure, and that every entry's bytes hash to its CID. |
| `ValidatedVolume` | Structure only. The entries are not hashed again. |

A `ValidatedVolume` is for bytes the caller already checked or made itself.
`ValidatedVolume(validating:)` does the hash check; `ValidatedVolume(trusting:)`
takes the caller's word. Bytes read back from a store do not qualify: a store
serves damaged bytes as they are.

cashew produces valid Volume boundaries and submits one Volume per storer
callback:

```swift
// Store the root and one selected nested Volume.
try await root.store(
    paths: [["accounts", "alice"]: .targeted],
    storer: disk
)
```

`.recursive` selects every nested Volume below a path. Each emitted Volume is
still an independent storage and retention unit.

A caller that already holds an all-or-none batch uses `storeVolumesLocal` to
commit it in one transaction.

## Reading

```swift
let disk = try DiskBroker(path: "/var/lib/my-app/volumes.sqlite")
let memory = MemoryBroker(near: disk)

let resolved = try await unresolvedRoot.resolveRecursive(source: memory)
```

Reads try memory and then disk. Stores remain explicit.

`VolumeBroker.fetch(_:)` resolves each cashew frontier with one batched read
per storage tier.

A read is one snapshot of committed state: reads run on a connection of their
own.

## Retention

A scope is a name. A retained root is a complete stored Volume root. A Volume
is kept if a scope retains or counts its root (see
[Counted roots](#counted-roots)), and only then. Retaining a root does not
keep a Volume nested under it: a caller that wants one kept retains its root
too.

| Call | Effect |
| --- | --- |
| `advanceRetainedRoots(scope:roots:)` | Atomically replaces the scope's root set. |
| `mergeRetainedRoots(scope:roots:)` | Adds roots without replacing the set. |
| `releaseRetainedRoots(scope:roots:)` | Takes roots out of a scope and removes those of them no scope retains or counts. |
| `releaseRetainedScopes(_:)` | Lets go of several scopes, everything each retains, in one transaction. |
| `retainedScopes(prefix:)` | Lists the scopes that retain something. |
| `sweep()` | Removes every Volume no scope retains or counts. |

### Advance and merge

```swift
try await disk.advanceRetainedRoots(scope: "canonical", roots: [stateRoot])
```

Advancing refuses any root that is not a complete stored Volume, so a
retained root never names content the broker lacks. Replacing a scope with
the same set and merging roots already in it are naturally idempotent.

### Sweep

```swift
let removed = try await disk.sweep()
```

`sweep` removes every Volume no scope retains or counts, then every CAS row
no surviving Volume owns, in one transaction. It has no grace window: content
stored but not yet retained is swept, and a later advance naming it is
refused. Callers store, then advance, and sweep only between their own
commits.

### Release

```swift
let removed = try await disk.releaseRetainedRoots(scope: "canonical", roots: [oldRoot])
```

`releaseRetainedRoots` removes the released Volumes that nothing else keeps,
with the CAS rows that leaves unowned. Its work is in the roots named, not in
the size of the store, and it touches nothing else.

A caller that keeps one scope per thing it holds uses
`retainedScopes(prefix:)` to find the scopes it no longer has a reason for,
and `releaseRetainedScopes` to let go of them.

### Scope rules

- A scope's name is its bytes. A prefix is a prefix of bytes, the listing is
  in byte order, and a name with a NUL is refused.
- A scope is let go of with `releaseRetainedRoots`, naming what it retains.
  Emptying one with `advanceRetainedRoots(scope:roots: [])` removes it from
  the listing and reclaims nothing: its Volumes stay until a sweep, or a
  release that names them.

### Storing and retaining together

Storing and then retaining are two commits. A release or a sweep between them
removes what was stored, and the retain is then refused whole with
`missingRetainedRoot`: store again and retry.

`storeVolumesLocal(_:retaining:)` makes them one commit: the batch is stored
and each scope's roots merged in one transaction, or nothing changes.

## Counted roots

A `CountedRootBroker` also keeps, per scope, a count for each root and one
marker, and changes them together:

```swift
try await disk.adjustCountedRoots(
    scope: "state", deltas: [newNode: 1, oldNode: -1],
    marker: "block-12", expectedMarker: "block-11")
```

The adjustment is one transaction. It is refused whole, with nothing changed,
in these cases:

| Case | Error |
| --- | --- |
| The scope's marker is not `expectedMarker` (`nil` for none) | `countedMarkerMismatch` |
| A count would go below zero | `negativeCount` |
| A count would go past `Int.max` | `countedRootOverflow` |
| A root would start being counted and is not a complete stored Volume | `countedRootsMissing`, listing every such root |

Zero deltas are ignored. A root whose count reaches zero leaves the scope
and, if no scope counts or retains it, is removed with the CAS rows that
leaves unowned. The marker lets a caller keep its own cursor in the same
commit as its counts.

A Volume is kept while any scope counts it or any scope retains it: release,
sweep, adjustment and drop all check both. Counted scopes are a namespace of
their own.

| Call | Effect |
| --- | --- |
| `countedState(scope:)` | Reads one scope's marker and counts from one snapshot. |
| `countedScopes(prefix:)` | Lists the scopes with a marker, by the same byte rules as `retainedScopes(prefix:)`. |
| `dropCountedScope(scope:)` | Deletes a scope's counts and marker and reclaims what nothing else keeps. |

An adjustment computed from the counts `countedState` returned passes that
marker as `expectedMarker`, so it applies only to the counts it was computed
from.

## Brokers

- `MemoryBroker` is the in-memory twin with identical retention and counting
  semantics.
- `DiskBroker` uses SQLite with WAL and foreign keys, and validates its
  schema when it opens.

Every broker is directly usable as cashew's `VolumeStorer`, `ContentSource`
and `Fetcher`.

### Durability

`DiskBroker` is opened in one of two modes.

| `durability` | SQLite setting | A commit that returns |
| --- | --- | --- |
| `.everyCommit` (default) | `synchronous=FULL` | Was fsynced to the WAL first, so a `store` that returns survives a crash. |
| `.deferred` | `synchronous=NORMAL` | Did not wait for its fsync. It is durable from the next `flush()` that returns. |

In `.deferred` mode writes stay atomic and ordered. A crash of the process
loses nothing, and a power loss loses the writes since the last `flush()`.
That suits a store whose content can be fetched again.

On a deferred broker the caller flushes:

1. before another store makes durable anything that names this content, and
2. before acknowledging a retained or counted root it promised.

### Schema

The schema version is 3, kept in SQLite's `user_version`.

| Table | Key | Holds |
| --- | --- | --- |
| `cas_data` | `cid` | The bytes of each CID. |
| `volume_metadata` | `root` | Each Volume's declared entry count. |
| `volume_entries` | `(root, cid)` | Which CIDs belong to which Volume. |
| `retained_roots` | `(scope, root)` | The roots each scope retains. |
| `counted_roots` | `(scope, root)` | The count of each root a scope counts. |
| `counted_markers` | `scope` | Each counted scope's marker. |

`volume_metadata` and `counted_markers`, whose small rows are found only by
their keys, are stored in those keys (`WITHOUT ROWID`). The other tables keep
their rowids, so rows append in order and the random-order indexes stay
narrow.

Each index serves a lookup by a column no key starts with:

| Index | Finds |
| --- | --- |
| `idx_ve_cid` | A CAS row's owners, by CID. |
| `idx_retained_roots_root` | The scopes that retain a root, by root. |
| `idx_counted_roots_root` | The scopes that count a root, by root. |

### Opening a database

`DiskBroker` initializes only an empty database. A database at any other
schema version fails closed with `migrationRequired`. A database at the
current version whose schema is malformed fails closed with `invalidSchema`.
Neither is modified.

## Upgrading

There is no migration between schema versions. When opening fails with
`migrationRequired`, rebuild from a new database path.
