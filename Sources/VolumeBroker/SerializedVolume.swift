import CID
import Foundation
import Multihash
import cashew

public enum SerializedVolumeError: Error, Equatable, Sendable {
    case emptyRoot
    case missingRootEntry(String)
    case invalidCID(String)
    case contentAddressMismatch(String)
}

public typealias SerializedVolume = cashew.SerializedVolume

public extension SerializedVolume {
    /// Enforces only generic storage invariants. The broker does not interpret the
    /// application's DAG or decide which nested Volumes an operation requires.
    ///
    /// A stored Volume must contain its declared root and every `(CID, bytes)` pair
    /// must be self-authenticating.
    func validate() throws {
        try validateStructure()
        for (rawCID, data) in entries {
            try Self.validate(cid: rawCID, data: data)
        }
    }
}

/// A Volume a broker stores without hashing its entries again: its
/// structure is checked, and its bytes are known to match their CIDs.
///
/// Bytes are hashed once, where they enter. `init(validating:)` is that
/// check, for bytes from anywhere the caller does not trust: a peer, an
/// operator, another process. `init(trusting:)` takes the caller's word,
/// for bytes this process has already hashed against their CIDs: ones it
/// serialized from its own nodes, or ones it validated before. Bytes read
/// back from a store are not that: a store serves damaged bytes as they
/// are. Either way the Volume holds its own copy of the bytes, so what was
/// checked is what is stored.
public struct ValidatedVolume: Sendable {
    public let volume: SerializedVolume

    /// Checks the Volume's structure and that every entry's bytes hash to
    /// its CID.
    public init(validating volume: SerializedVolume) throws {
        let owned = volume.ownedCopy()
        try owned.validate()
        self.volume = owned
    }

    /// Checks the Volume's structure only: every entry's bytes are taken
    /// to hash to its CID, unchecked.
    public init(trusting volume: SerializedVolume) throws {
        let owned = volume.ownedCopy()
        try owned.validateStructure()
        self.volume = owned
    }
}

extension SerializedVolume {
    func validateStructure() throws {
        guard !root.isEmpty else { throw SerializedVolumeError.emptyRoot }
        guard entries[root] != nil else { throw SerializedVolumeError.missingRootEntry(root) }
    }
}

extension SerializedVolume {
    /// Validate one self-authenticating CAS entry without constructing or
    /// traversing a whole Volume.
    static func validate(cid rawCID: String, data: Data) throws {
        let expected = try parsedCID(rawCID)
        let canonical: CID
        do {
            canonical = try CID(
                version: expected.version,
                codec: expected.codec,
                multihash: expected.multihash
            )
        } catch {
            throw SerializedVolumeError.invalidCID(rawCID)
        }
        guard canonical.toBaseEncodedString == rawCID else {
            throw SerializedVolumeError.invalidCID(rawCID)
        }
        guard let algorithm = expected.multihash.algorithm,
              let digestLength = expected.multihash.length,
              let expectedDigest = expected.multihash.digest,
              digestLength > 0 else {
            throw SerializedVolumeError.invalidCID(rawCID)
        }
        if expected.version == .v0 {
            guard expected.codec == .dag_pb,
                  algorithm == .sha2_256,
                  digestLength == 32 else {
                throw SerializedVolumeError.invalidCID(rawCID)
            }
        }

        if algorithm == .identity {
            guard expectedDigest.count == digestLength,
                  Data(expectedDigest) == data else {
                throw SerializedVolumeError.contentAddressMismatch(rawCID)
            }
            return
        }

        let actualMultihash: Multihash
        do {
            actualMultihash = try Multihash(
                raw: data,
                hashedWith: algorithm,
                customByteLength: digestLength
            )
        } catch {
            throw SerializedVolumeError.invalidCID(rawCID)
        }

        let actual: CID
        do {
            actual = try CID(
                version: expected.version,
                codec: expected.codec,
                multihash: actualMultihash
            )
        } catch {
            throw SerializedVolumeError.invalidCID(rawCID)
        }

        guard actual == expected else {
            throw SerializedVolumeError.contentAddressMismatch(rawCID)
        }
    }

    private static func parsedCID(_ rawCID: String) throws -> CID {
        do {
            return try CID(rawCID)
        } catch {
            throw SerializedVolumeError.invalidCID(rawCID)
        }
    }
}

extension SerializedVolume {
    func ownedCopy() -> SerializedVolume {
        SerializedVolume(root: root, entries: entries.mapValues { data in
            data.withUnsafeBytes { Data($0) }
        })
    }
}
