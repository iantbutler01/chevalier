import Foundation

struct CloneRequest: Codable, Equatable {
  static let supportedSchemaVersion = 1

  let schemaVersion: Int
  let sourceBundlePath: String
  let destinationBundlePath: String

  var sourceBundleURL: URL {
    Self.fileURL(sourceBundlePath)
  }

  var destinationBundleURL: URL {
    Self.fileURL(destinationBundlePath)
  }

  func validateShape() throws {
    guard schemaVersion == Self.supportedSchemaVersion else {
      throw CloneError.unsupportedRequestSchema(schemaVersion)
    }
    guard !sourceBundlePath.isEmpty else {
      throw CloneError.invalidRequest("sourceBundlePath must not be empty")
    }
    guard !destinationBundlePath.isEmpty else {
      throw CloneError.invalidRequest("destinationBundlePath must not be empty")
    }
    guard NSString(string: sourceBundlePath).expandingTildeInPath.hasPrefix("/") else {
      throw CloneError.invalidRequest("sourceBundlePath must be absolute")
    }
    guard NSString(string: destinationBundlePath).expandingTildeInPath.hasPrefix("/") else {
      throw CloneError.invalidRequest("destinationBundlePath must be absolute")
    }
    guard sourceBundleURL != destinationBundleURL else {
      throw CloneError.invalidRequest("source and destination bundle paths must differ")
    }
    guard !destinationBundleURL.path.hasPrefix(sourceBundleURL.path + "/") else {
      throw CloneError.invalidRequest("destination bundle must not be inside source bundle")
    }
    guard destinationBundleURL.path != "/" else {
      throw CloneError.invalidRequest("destinationBundlePath must not be the filesystem root")
    }
  }

  private static func fileURL(_ path: String) -> URL {
    URL(
      fileURLWithPath: NSString(string: path).expandingTildeInPath,
      isDirectory: true
    ).standardizedFileURL
  }
}

struct CloneResultReport: Codable, Equatable {
  let schemaVersion: Int
  let sourceBundlePath: String
  let destinationBundlePath: String
  let diskCloneMethod: String
  let diskSizeBytes: UInt64
  let hardwareModelSHA256: String
  let machineIdentifierSHA256: String
  let auxiliaryStorageSHA256: String
}

enum CloneError: Error, Equatable, LocalizedError {
  case unsupportedRequestSchema(Int)
  case invalidRequest(String)
  case sourceBundleInvalid(String)
  case destinationAlreadyExists(String)
  case crossDevice(source: String, destinationParent: String)
  case cloneFileFailed(source: String, destination: String, errno: Int32)
  case io(String)

  var errorDescription: String? {
    switch self {
    case .unsupportedRequestSchema(let schema):
      "unsupported clone request schema: \(schema)"
    case .invalidRequest(let message):
      message
    case .sourceBundleInvalid(let message):
      "invalid source bundle: \(message)"
    case .destinationAlreadyExists(let path):
      "destination bundle already exists: \(path)"
    case .crossDevice(let source, let destinationParent):
      "APFS clone requires source \(source) and destination parent \(destinationParent) on the same device"
    case .cloneFileFailed(let source, let destination, let errorNumber):
      "unable to APFS-clone \(source) to \(destination): errno \(errorNumber)"
    case .io(let message):
      "clone I/O failed: \(message)"
    }
  }
}
