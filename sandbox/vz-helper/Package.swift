// swift-tools-version: 6.3
import PackageDescription

let package = Package(
  name: "ChevalierVZ",
  platforms: [.macOS(.v14)],
  products: [
    .executable(name: "chevalier-vz", targets: ["ChevalierVZ"])
  ],
  targets: [
    .executableTarget(
      name: "ChevalierVZ"
    ),
    .testTarget(
      name: "ChevalierVZTests",
      dependencies: ["ChevalierVZ"]
    ),
  ],
  swiftLanguageModes: [.v6]
)
