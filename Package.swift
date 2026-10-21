// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "VolumeBroker",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [
        .library(name: "VolumeBroker", targets: ["VolumeBroker"]),
    ],
    dependencies: [
        .package(url: "https://github.com/adalinxx/cashew.git", exact: "5.0.1"),
        .package(url: "https://github.com/swift-libp2p/swift-cid.git", exact: "0.2.1"),
        .package(url: "https://github.com/swift-libp2p/swift-multihash.git", exact: "0.2.1"),
    ],
    targets: [
        .systemLibrary(name: "VolumeBrokerSQLite"),
        .target(
            name: "VolumeBroker",
            dependencies: [
                "VolumeBrokerSQLite",
                .product(name: "cashew", package: "cashew"),
                .product(name: "CID", package: "swift-cid"),
                .product(name: "Multihash", package: "swift-multihash"),
            ]
        ),
        .testTarget(
            name: "VolumeBrokerTests",
            dependencies: [
                "VolumeBroker",
                "VolumeBrokerSQLite",
                .product(name: "cashew", package: "cashew"),
                .product(name: "CID", package: "swift-cid"),
                .product(name: "Multihash", package: "swift-multihash"),
            ]
        ),
        .testTarget(
            name: "VolumeBrokerBenchmarks",
            dependencies: [
                "VolumeBroker",
                .product(name: "CID", package: "swift-cid"),
                .product(name: "Multihash", package: "swift-multihash"),
            ]
        ),
    ]
)
