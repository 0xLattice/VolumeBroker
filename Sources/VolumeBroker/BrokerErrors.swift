public enum BrokerError: Error, Sendable, Equatable {
    case openFailed(String)
    case sqlFailed(String)
    case migrationRequired(found: Int, required: Int)
    case invalidSchema(version: Int)
    case notFound
    case invalidRetainedRoots(String)
    case missingRetainedRoot(String)
    case inconsistentState(String)

    /// A counted adjustment found a different marker than it expected
    /// (`nil` is no marker). Nothing changed.
    case countedMarkerMismatch(expected: String?, found: String?)

    /// A counted adjustment would take this root's count below zero. Nothing
    /// changed.
    case negativeCount(String)

    /// A counted adjustment would take this root's count past `Int.max`.
    /// Nothing changed.
    case countedRootOverflow(String)

    /// A counted marker is empty or holds a NUL.
    case invalidCountedMarker(String)

    /// A counted adjustment would start counting these roots, sorted, and
    /// none of them is a complete stored Volume. Nothing changed.
    case countedRootsMissing([String])

    /// Existing CAS bytes for a CID differ from the bytes being stored. Under
    /// content addressing this is corruption (or an impossible hash collision),
    /// never a benign overwrite.
    case conflictingContent(String)

    /// A published Volume root cannot later name a different complete entry set.
    case conflictingVolume(String)
}
