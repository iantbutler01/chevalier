import Foundation

struct InstallRequest: Codable, Equatable {
  static let supportedSchemaVersion = 1
  static let minimumRootDiskSizeBytes: UInt64 = 64 * 1024 * 1024 * 1024

  let schemaVersion: Int
  let bundlePath: String
  let ipswPath: String
  let expectedSHA256: String
  let rootDiskSizeBytes: UInt64
  let requiredFreeSpaceBytes: UInt64
  let cpuCount: Int
  let memoryBytes: UInt64

  var bundleURL: URL {
    URL(fileURLWithPath: NSString(string: bundlePath).expandingTildeInPath, isDirectory: true)
      .standardizedFileURL
  }

  var ipswURL: URL {
    URL(fileURLWithPath: NSString(string: ipswPath).expandingTildeInPath).standardizedFileURL
  }

  func validateShape() throws {
    guard schemaVersion == Self.supportedSchemaVersion else {
      throw InstallError.unsupportedRequestSchema(schemaVersion)
    }
    guard !bundlePath.isEmpty else {
      throw InstallError.invalidRequest("bundlePath must not be empty")
    }
    guard !ipswPath.isEmpty else {
      throw InstallError.invalidRequest("ipswPath must not be empty")
    }
    let digest = expectedSHA256.lowercased()
    guard digest.count == 64, digest.allSatisfy(\.isHexDigit) else {
      throw InstallError.invalidRequest("expectedSHA256 must be a 64-character hexadecimal digest")
    }
    guard rootDiskSizeBytes >= Self.minimumRootDiskSizeBytes else {
      throw InstallError.invalidRequest(
        "rootDiskSizeBytes must be at least \(Self.minimumRootDiskSizeBytes)")
    }
    guard requiredFreeSpaceBytes > 0 else {
      throw InstallError.invalidRequest("requiredFreeSpaceBytes must be greater than zero")
    }
    guard cpuCount > 0 else {
      throw InstallError.invalidRequest("cpuCount must be greater than zero")
    }
    guard memoryBytes > 0 else {
      throw InstallError.invalidRequest("memoryBytes must be greater than zero")
    }
  }
}

enum InstallError: Error, Equatable {
  case unsupportedRequestSchema(Int)
  case invalidRequest(String)
  case restoreImageNotRegularFile(String)
  case restoreImageChecksumMismatch(expected: String, actual: String)
  case unsupportedRestoreImage(version: String, build: String)
  case missingConfigurationRequirements(version: String, build: String)
  case bundleAlreadyExists(String)
  case eventFileAlreadyExists(String)
  case insufficientFreeSpace(path: String, required: UInt64, available: UInt64)
  case resourceOutsideAllowedRange(
    resource: String, requested: UInt64, minimum: UInt64, maximum: UInt64)
  case unableToCreateSparseDisk(path: String, errno: Int32)
  case invalidConfiguration(String)
  case progressOutputFailed(String)
}

struct InstallResultReport: Codable, Equatable {
  let schemaVersion: Int
  let bundlePath: String
  let guestOperatingSystemVersion: String
  let guestBuildVersion: String
  let ipswSHA256: String
  let hardwareModelSHA256: String
  let machineIdentifierSHA256: String
  let rootDiskSizeBytes: UInt64
  let cpuCount: Int
  let memoryBytes: UInt64
  let saveRestoreSupported: Bool
  let saveRestoreValidationError: String?
}

struct InstallEvent: Codable, Equatable {
  let schemaVersion: Int
  let sequence: UInt64
  let type: String
  let timestamp: String
  let phase: String
  let fractionCompleted: Double
  let message: String?
}

struct TemplateManifest: Codable, Equatable {
  let schemaVersion: Int
  let status: String
  let bundlePath: String
  let guestOperatingSystemVersion: String
  let guestBuildVersion: String
  let architecture: String
  let ipswSHA256: String
  let hardwareModelSHA256: String
  let templateMachineIdentifierSHA256: String
  let rootDiskSizeBytes: UInt64
  let cpuCount: Int
  let memoryBytes: UInt64
  let deviceProfile: String
  let failure: String?
}

struct TemplateProvenance: Codable, Equatable {
  let schemaVersion: Int
  let ipswPath: String
  let ipswSizeBytes: UInt64
  let ipswSHA256: String
  let guestOperatingSystemVersion: String
  let guestBuildVersion: String
  let hardwareModelSHA256: String
  let createdAt: String
}
