import Foundation

struct GuestServiceRelayRequest: Codable, Equatable {
  let vsockPort: UInt32
  let hostLoopbackPort: UInt16
}

struct SharedDirectoryRequest: Codable, Equatable {
  let hostPath: String
  let name: String
  let readOnly: Bool

  var hostURL: URL {
    URL(
      fileURLWithPath: NSString(string: hostPath).expandingTildeInPath,
      isDirectory: true
    ).standardizedFileURL
  }
}

enum RunNetworkMode: String, Codable, Equatable {
  case none
  case natDevelopment
}

enum RunViewerMode: String, Codable, Equatable {
  case headless
  case window
}

struct RunRequest: Codable, Equatable {
  static let supportedSchemaVersion = 1
  static let controlVSockPort: UInt32 = 13_338

  let schemaVersion: Int
  let bundlePath: String
  let cpuCount: Int
  let memoryBytes: UInt64
  let provisioningDirectoryPath: String?
  let provisioningDirectoryReadOnly: Bool?
  let networkMode: RunNetworkMode?
  let viewerMode: RunViewerMode?
  let loopbackRelayPort: UInt16?
  let guestIngressRelayPort: UInt16?
  let guestServiceRelays: [GuestServiceRelayRequest]?
  let sharedDirectories: [SharedDirectoryRequest]?
  let ownerControlSocketPath: String?
  let runtimeGeneration: String?

  init(
    schemaVersion: Int,
    bundlePath: String,
    cpuCount: Int,
    memoryBytes: UInt64,
    provisioningDirectoryPath: String?,
    provisioningDirectoryReadOnly: Bool?,
    networkMode: RunNetworkMode?,
    viewerMode: RunViewerMode?,
    loopbackRelayPort: UInt16?,
    guestIngressRelayPort: UInt16? = nil,
    guestServiceRelays: [GuestServiceRelayRequest]?,
    sharedDirectories: [SharedDirectoryRequest]? = nil,
    ownerControlSocketPath: String? = nil,
    runtimeGeneration: String? = nil
  ) {
    self.schemaVersion = schemaVersion
    self.bundlePath = bundlePath
    self.cpuCount = cpuCount
    self.memoryBytes = memoryBytes
    self.provisioningDirectoryPath = provisioningDirectoryPath
    self.provisioningDirectoryReadOnly = provisioningDirectoryReadOnly
    self.networkMode = networkMode
    self.viewerMode = viewerMode
    self.loopbackRelayPort = loopbackRelayPort
    self.guestIngressRelayPort = guestIngressRelayPort
    self.guestServiceRelays = guestServiceRelays
    self.sharedDirectories = sharedDirectories
    self.ownerControlSocketPath = ownerControlSocketPath
    self.runtimeGeneration = runtimeGeneration
  }

  var effectiveViewerMode: RunViewerMode {
    viewerMode ?? .headless
  }

  var bundleURL: URL {
    Self.fileURL(bundlePath, isDirectory: true)
  }

  var provisioningDirectoryURL: URL? {
    provisioningDirectoryPath.map { Self.fileURL($0, isDirectory: true) }
  }

  var ownerControlSocketURL: URL? {
    ownerControlSocketPath.map { Self.fileURL($0, isDirectory: false) }
  }

  func validateShape() throws {
    guard schemaVersion == Self.supportedSchemaVersion else {
      throw RunError.unsupportedRequestSchema(schemaVersion)
    }
    guard !bundlePath.isEmpty else {
      throw RunError.invalidRequest("bundlePath must not be empty")
    }
    guard cpuCount > 0 else {
      throw RunError.invalidRequest("cpuCount must be greater than zero")
    }
    guard memoryBytes > 0 else {
      throw RunError.invalidRequest("memoryBytes must be greater than zero")
    }
    if let provisioningDirectoryPath, provisioningDirectoryPath.isEmpty {
      throw RunError.invalidRequest("provisioningDirectoryPath must not be empty when present")
    }
    if loopbackRelayPort == 0 {
      throw RunError.invalidRequest("loopbackRelayPort must be greater than zero when present")
    }
    if guestIngressRelayPort == 0 {
      throw RunError.invalidRequest("guestIngressRelayPort must be greater than zero when present")
    }
    if let loopbackRelayPort, loopbackRelayPort == guestIngressRelayPort {
      throw RunError.invalidRequest(
        "guestIngressRelayPort must not collide with loopbackRelayPort")
    }
    if let ownerControlSocketPath {
      guard !ownerControlSocketPath.isEmpty else {
        throw RunError.invalidRequest("ownerControlSocketPath must not be empty when present")
      }
      guard NSString(string: ownerControlSocketPath).expandingTildeInPath.hasPrefix("/") else {
        throw RunError.invalidRequest("ownerControlSocketPath must be absolute")
      }
      guard let runtimeGeneration, !runtimeGeneration.isEmpty else {
        throw RunError.invalidRequest(
          "runtimeGeneration must be nonempty when ownerControlSocketPath is present")
      }
    }
    if let runtimeGeneration {
      guard !runtimeGeneration.isEmpty, runtimeGeneration.utf8.count <= 256 else {
        throw RunError.invalidRequest("runtimeGeneration must contain 1...256 UTF-8 bytes")
      }
    }

    var vsockPorts = Set<UInt32>()
    for relay in guestServiceRelays ?? [] {
      guard relay.vsockPort > 0 else {
        throw RunError.invalidRequest("guestServiceRelays vsockPort must be greater than zero")
      }
      guard relay.hostLoopbackPort > 0 else {
        throw RunError.invalidRequest(
          "guestServiceRelays hostLoopbackPort must be greater than zero")
      }
      guard relay.vsockPort != Self.controlVSockPort else {
        throw RunError.invalidRequest(
          "guestServiceRelays vsockPort must not collide with control port \(Self.controlVSockPort)"
        )
      }
      guard vsockPorts.insert(relay.vsockPort).inserted else {
        throw RunError.invalidRequest(
          "guestServiceRelays contains duplicate vsockPort \(relay.vsockPort)")
      }
    }

    if provisioningDirectoryPath != nil, !(sharedDirectories ?? []).isEmpty {
      throw RunError.invalidRequest(
        "provisioningDirectoryPath and sharedDirectories cannot be used together")
    }

    var directoryNames = Set<String>()
    for directory in sharedDirectories ?? [] {
      guard !directory.hostPath.isEmpty, directory.hostURL.path.hasPrefix("/") else {
        throw RunError.invalidRequest("sharedDirectories hostPath must be absolute")
      }
      guard !directory.name.isEmpty else {
        throw RunError.invalidRequest("sharedDirectories name must not be empty")
      }
      guard directoryNames.insert(directory.name).inserted else {
        throw RunError.invalidRequest(
          "sharedDirectories contains duplicate name \(directory.name)")
      }
    }
  }

  private static func fileURL(_ path: String, isDirectory: Bool) -> URL {
    URL(
      fileURLWithPath: NSString(string: path).expandingTildeInPath,
      isDirectory: isDirectory
    ).standardizedFileURL
  }
}

enum RunError: Error, Equatable, LocalizedError {
  case unsupportedRequestSchema(Int)
  case invalidRequest(String)
  case missingBundleArtifact(String)
  case invalidBundleArtifact(String)
  case provisioningDirectoryNotFound(String)
  case sharedDirectoryNotFound(String)
  case invalidConfiguration(String)
  case missingVirtioSocketDevice
  case loopbackRelay(String)

  var errorDescription: String? {
    switch self {
    case .unsupportedRequestSchema(let schema):
      "unsupported run request schema: \(schema)"
    case .invalidRequest(let message):
      message
    case .missingBundleArtifact(let path):
      "required VM bundle artifact is missing: \(path)"
    case .invalidBundleArtifact(let path):
      "VM bundle artifact is invalid: \(path)"
    case .provisioningDirectoryNotFound(let path):
      "provisioning directory does not exist: \(path)"
    case .sharedDirectoryNotFound(let path):
      "shared directory does not exist: \(path)"
    case .invalidConfiguration(let message):
      "invalid run configuration: \(message)"
    case .missingVirtioSocketDevice:
      "run configuration did not create a Virtio socket device"
    case .loopbackRelay(let message):
      "loopback relay failed: \(message)"
    }
  }
}
