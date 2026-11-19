# Correctness invariants

"Published" means visible to presence, whole-Volume fetch, and per-CID fetch.
Retention never substitutes for publication.

| ID | Law |
| --- | --- |
| VOLUME-001 | The declared root is present in the Volume. |
| VOLUME-002 | Every `(CID, bytes)` pair uses the repository's canonical CID spelling and validates using its declared digest length: checked by the broker for a `SerializedVolume`, and by its maker for a `ValidatedVolume`. |
| VOLUME-003 | A batch owns a copied snapshot and publishes all valid Volumes or none. |
| VOLUME-004 | Valid existing CID content cannot acquire different bytes; invalid local bytes may be repaired only by submitted bytes that authenticate to that CID, as VOLUME-002 checks them. |
| VOLUME-005 | An existing Volume root cannot acquire different membership. |
| VOLUME-006 | Declared count, membership, and root membership must agree before publication. Foreign keys guarantee every member has a CAS row and metadata; the schema guarantees an integer, positive declared count. |
| VOLUME-007 | Cashew completes one independently durable selected Volume per `VolumeStorer` callback; a caller with a preassembled all-or-none batch may use `storeVolumesLocal`. |
| VOLUME-008 | Loose CAS rows (only a writer outside this library, with foreign keys off, can leave one; nothing collects it) and incomplete membership grant no visibility. |
| VOLUME-009 | Retained-root sets accept only complete local roots, all-or-none. A Volume is kept if a scope retains or counts its root, and only then: retaining or counting a root does not keep a Volume nested under it. `MemoryBroker` sweeps by the same rule. |
| VOLUME-010 | Retained-set replacement and merge are naturally idempotent. Advance replaces the whole set, so a caller that both merges and advances one scope serializes them. |
| VOLUME-011 | `sweep` removes every Volume no scope retains or counts and then the CAS rows those Volumes leave unowned, in one transaction, serialized with stores and advances; it has no grace window. Every commit leaves each CAS row owned, so those rows are the ones the removed Volumes leave unowned, and the sweep reaches them through those Volumes, not a scan of the store. A store edited outside the library with foreign keys off is not checked at open; repairing it is an explicit job, not the sweep's. |
| VOLUME-012 | Shared CAS bytes remain until their last owning Volume is removed. |
| VOLUME-013 | Immutable `near` and `far` links are read tiers in one domain; writes and cross-domain synchronization are explicit. |
| VOLUME-014 | Empty v0 initializes atomically; malformed, nonempty, and unsupported schemas are not mutated. |
| VOLUME-015 | Chain state, canonicity, and application metadata cannot bypass the Volume contract. |
| VOLUME-016 | Presence, retention and reads use the structural gate. Bytes are hashed once, on their way in: a `SerializedVolume` is validated when stored, and a `ValidatedVolume` is stored on its maker's word, its structure checked and its entries not hashed again. A read returns the bytes as stored, unhashed. |
| VOLUME-017 | Bytes damaged in the store are served as they are, for whoever checks them against their CID to catch; nothing is flagged or deleted, what is retained is unchanged, and a re-store of the right bytes overwrites them. |
| VOLUME-018 | By default SQLite WAL with `synchronous=FULL` is the durable-commit boundary: a returned store survives a crash. Opened `.deferred` (`synchronous=NORMAL`), a returned store is durable from the next `flush()` that returns. In both modes writes are atomic and ordered in one WAL, so the store after any crash is a prefix of the committed writes, each whole, and a later write in this store never names content an earlier one lost. Across stores the caller keeps the order: on a deferred broker, anything another store makes durable that names this content, and an acknowledged retained or counted root, comes after a `flush()`. A `flush()` that throws leaves durability unknown. |
| VOLUME-019 | A batched CID read applies the same complete-owner serve gate and local-to-far precedence as scalar reads. |
| VOLUME-020 | `releaseRetainedRoots` removes the named roots from one scope, then each of those Volumes no scope retains or counts and the CAS rows that leaves unowned, in one transaction. It touches no Volume it was not given, and its work is in the roots named. |
| VOLUME-021 | A scope is named by the bytes of its name, which hold no NUL, and exists while it retains a root. `retainedScopes(prefix:)` lists those whose name starts with the prefix, in byte order, the same on both brokers; a NUL in a read names no scope. On disk the listing costs the scopes listed, not the roots retained. |
| VOLUME-022 | Reads run on a read-only connection of their own, never the write connection: a read is a snapshot of committed state and sees no transaction half done. Opening creates the WAL files that connection needs, and fails if it cannot be had. |
| VOLUME-023 | A counted adjustment changes one scope's counts and marker in one transaction, and is refused whole on a marker other than the one expected, a count below zero or past `Int.max`, or a root starting to be counted that is not a complete stored Volume (every such root is listed). A count is above zero or absent. `countedState` reads a scope's marker and counts from one snapshot. |
| VOLUME-024 | A Volume is kept while a scope retains it or counts it. Release, sweep, adjustment and scope drop each remove a Volume only when neither holds it, with the CAS rows that leaves unowned. |

A read never writes, so a database error or a corrupt leaf can never shrink
what a retained root keeps.

Primary coverage: `VolumeIntegrityTests`, `MemoryBrokerTests`,
`DiskBrokerTests`, `RetentionTests`, `SweepTests`, `CountedRootsTests`,
`SchemaVersionTests`, and `CashewProtocolTests`.
