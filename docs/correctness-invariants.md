# Correctness invariants

"Published" means visible to presence, whole-Volume fetch, and per-CID fetch.
Retention never substitutes for publication.

| ID | Law |
| --- | --- |
| VOLUME-001 | The declared root is present in the Volume. |
| VOLUME-002 | Every `(CID, bytes)` pair uses the repository's canonical CID spelling and validates using its declared digest length. |
| VOLUME-003 | A batch owns a copied snapshot and publishes all valid Volumes or none. |
| VOLUME-004 | Valid existing CID content cannot acquire different bytes; invalid local bytes may be repaired only by submitted bytes that authenticate to that CID. |
| VOLUME-005 | An existing Volume root cannot acquire different membership. |
| VOLUME-006 | Declared count, membership, and root membership must agree before publication. Foreign keys guarantee every member has a CAS row and metadata; the schema guarantees an integer, positive declared count. |
| VOLUME-007 | Cashew completes one independently durable selected Volume per `VolumeStorer` callback; a caller with a preassembled all-or-none batch may use `storeVolumesLocal`. |
| VOLUME-008 | Loose CAS rows and incomplete membership grant no visibility; requested bytes are returned only after CID validation. |
| VOLUME-009 | Retained-root sets accept only complete local roots, all-or-none. A Volume is kept if a scope retains its root, and only then: retaining a root does not keep a Volume nested under it. `MemoryBroker` sweeps by the same rule. |
| VOLUME-010 | Retained-set replacement and merge are naturally idempotent. Advance replaces the whole set, so a caller that both merges and advances one scope serializes them. |
| VOLUME-011 | `sweep` removes every unretained Volume and then every unowned CAS row in one transaction, serialized with stores and advances; it has no grace window. |
| VOLUME-012 | Shared CAS bytes remain until their last owning Volume is removed. |
| VOLUME-013 | Immutable `near` and `far` links are read tiers in one domain; writes and cross-domain synchronization are explicit. |
| VOLUME-014 | Empty v0 initializes atomically; malformed, nonempty, and unsupported schemas are not mutated. |
| VOLUME-015 | Chain state, canonicity, and application metadata cannot bypass the Volume contract. |
| VOLUME-016 | Presence and retention use the structural gate; a point read validates only its requested `(CID, bytes)`, while a whole-Volume read validates the whole Volume. |
| VOLUME-017 | Corrupt bytes fail CID validation on read and are not served; nothing is flagged or deleted, what is retained is unchanged, and a valid re-store overwrites them. |
| VOLUME-018 | SQLite WAL with `synchronous=FULL` is the durable-commit boundary: a returned store survives a crash, so a later advance never names lost content. |
| VOLUME-019 | A batched CID read applies the same complete-owner serve gate, CID validation and local-to-far precedence as scalar reads. |
| VOLUME-020 | `releaseRetainedRoots` removes the named roots from one scope, then each of those Volumes no scope retains and the CAS rows that leaves unowned, in one transaction. It touches no Volume it was not given, and its work is in the roots named. |

A read never writes: corruption only hides the corrupt bytes from readers, so a
database error or a corrupt leaf can never shrink what a retained root keeps.

Primary coverage: `VolumeIntegrityTests`, `MemoryBrokerTests`,
`DiskBrokerTests`, `RetentionTests`, `SweepTests`,
`SchemaVersionTests`, and `CashewProtocolTests`.
