// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "sixpn-service-discovery",
    platforms: [
        .macOS(.v12)
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "SixPNServiceDiscovery",
            targets: ["SixPNServiceDiscovery"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/apple/swift-service-discovery.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-atomics.git", .upToNextMajor(from: "1.0.0")),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/vapor/async-kit.git", from: "1.0.0"),
        .package(url: "https://github.com/pointfreeco/xctest-dynamic-overlay.git", from: "0.4.1"),
        .package(url: "https://github.com/OpenKitten/NioDNS.git", from: "2.0.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "SixPNServiceDiscovery",
            dependencies: [
                .product(name: "ServiceDiscovery", package: "swift-service-discovery"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "AsyncKit", package: "async-kit"),
                .product(name: "XCTestDynamicOverlay", package: "xctest-dynamic-overlay"),
                .product(name: "DNSClient", package: "NioDNS")
            ]
        ),
        .testTarget(
            name: "SixPNServiceDiscoveryTests",
            dependencies: [
                "SixPNServiceDiscovery",
                .product(name: "XCTestDynamicOverlay", package: "xctest-dynamic-overlay"),
                .product(name: "NIOCore", package: "swift-nio")
            ]
        ),
    ]
)
