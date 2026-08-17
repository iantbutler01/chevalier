import Foundation

struct VersionReport: Codable, Equatable {
  static let current = VersionReport(
    schemaVersion: 1,
    helperVersion: "0.4.0",
    protocolVersion: 1
  )

  let schemaVersion: Int
  let helperVersion: String
  let protocolVersion: Int
}

struct ProbeReport: Codable, Equatable {
  let schemaVersion: Int
  let helperVersion: String
  let protocolVersion: Int
  let host: HostReport
  let latestSupportedRestoreImage: RestoreImageReport
}

struct HostReport: Codable, Equatable {
  let architecture: String
  let operatingSystemVersion: String
  let operatingSystemBuild: String
  let virtualizationSupported: Bool
  let virtualizationEntitlementPresent: Bool
  let minimumAllowedCPUCount: Int
  let maximumAllowedCPUCount: Int
  let minimumAllowedMemoryBytes: UInt64
  let maximumAllowedMemoryBytes: UInt64
}

struct RestoreImageReport: Codable, Equatable {
  let schemaVersion: Int
  let source: String
  let url: String
  let isLocalFile: Bool
  let operatingSystemVersion: String
  let buildVersion: String
  let supported: Bool
  let configurationRequirements: ConfigurationRequirementsReport?
  let fileSizeBytes: UInt64?
  let sha256: String?
}

struct ConfigurationRequirementsReport: Codable, Equatable {
  let minimumSupportedCPUCount: Int
  let minimumSupportedMemoryBytes: UInt64
  let hardwareModelSupported: Bool
  let hardwareModelSHA256: String
}

struct ErrorReport: Codable, Equatable {
  let schemaVersion: Int
  let code: String
  let message: String
  let exitCode: Int32

  init(error: Error) {
    switch error {
    case CLIError.missingCommand:
      self.init(code: "missing_command", message: CLIParser.usage, exitCode: 64)
    case CLIError.missingValue(let option):
      self.init(code: "missing_value", message: "missing value for \(option)", exitCode: 64)
    case CLIError.invalidValue(let option, let value):
      self.init(
        code: "invalid_value", message: "invalid value for \(option): \(value)", exitCode: 64)
    case CLIError.unexpectedArgument(let argument):
      self.init(
        code: "unexpected_argument", message: "unexpected argument: \(argument)", exitCode: 64)
    case CLIError.unknownCommand(let command):
      self.init(code: "unknown_command", message: "unknown command: \(command)", exitCode: 64)
    case HelperError.notARegularFile(let path):
      self.init(code: "invalid_restore_image", message: "not a regular file: \(path)", exitCode: 66)
    case HelperError.unsupportedHostArchitecture:
      self.init(
        code: "unsupported_host_architecture", message: "macOS guests require an arm64 host",
        exitCode: 69)
    case HelperError.checksumMismatch(let expected, let actual):
      self.init(
        code: "checksum_mismatch",
        message: "restore image SHA-256 mismatch: expected \(expected), got \(actual)",
        exitCode: 65)
    case HelperError.unsupportedRestoreImage(let version, let build):
      self.init(
        code: "unsupported_restore_image",
        message: "macOS restore image \(version) (\(build)) is unsupported on this host",
        exitCode: 69)
    case HelperError.missingConfigurationRequirements(let version, let build):
      self.init(
        code: "missing_configuration_requirements",
        message: "macOS restore image \(version) (\(build)) has no supported configuration",
        exitCode: 69)
    case InstallError.unsupportedRequestSchema(let schema):
      self.init(
        code: "invalid_install_request", message: "unsupported install request schema: \(schema)",
        exitCode: 64)
    case InstallError.invalidRequest(let message):
      self.init(code: "invalid_install_request", message: message, exitCode: 64)
    case InstallError.restoreImageNotRegularFile(let path):
      self.init(code: "invalid_restore_image", message: "not a regular file: \(path)", exitCode: 66)
    case InstallError.restoreImageChecksumMismatch(let expected, let actual):
      self.init(
        code: "checksum_mismatch",
        message: "restore image SHA-256 mismatch: expected \(expected), got \(actual)",
        exitCode: 65)
    case InstallError.unsupportedRestoreImage(let version, let build):
      self.init(
        code: "unsupported_restore_image",
        message: "macOS restore image \(version) (\(build)) is unsupported on this host",
        exitCode: 69)
    case InstallError.missingConfigurationRequirements(let version, let build):
      self.init(
        code: "missing_configuration_requirements",
        message: "macOS restore image \(version) (\(build)) has no supported configuration",
        exitCode: 69)
    case InstallError.bundleAlreadyExists(let path):
      self.init(code: "destination_exists", message: "bundle already exists: \(path)", exitCode: 73)
    case InstallError.eventFileAlreadyExists(let path):
      self.init(
        code: "destination_exists", message: "event file already exists: \(path)", exitCode: 73)
    case InstallError.insufficientFreeSpace(let path, let required, let available):
      self.init(
        code: "insufficient_space",
        message: "insufficient free space at \(path): need \(required) bytes, have \(available)",
        exitCode: 73)
    case InstallError.resourceOutsideAllowedRange(
      let resource, let requested, let minimum, let maximum):
      self.init(
        code: "invalid_install_request",
        message: "\(resource) \(requested) is outside supported range \(minimum)...\(maximum)",
        exitCode: 64)
    case InstallError.unableToCreateSparseDisk(let path, let errorNumber):
      self.init(
        code: "io_error", message: "unable to create sparse disk at \(path): errno \(errorNumber)",
        exitCode: 74)
    case InstallError.invalidConfiguration(let message):
      self.init(code: "invalid_configuration", message: message, exitCode: 69)
    case InstallError.progressOutputFailed(let message):
      self.init(code: "io_error", message: "progress output failed: \(message)", exitCode: 74)
    case RunError.unsupportedRequestSchema(let schema):
      self.init(
        code: "invalid_run_request", message: "unsupported run request schema: \(schema)",
        exitCode: 64)
    case RunError.invalidRequest(let message):
      self.init(code: "invalid_run_request", message: message, exitCode: 64)
    case RunError.missingBundleArtifact(let path):
      self.init(
        code: "missing_bundle_artifact", message: "missing bundle artifact: \(path)", exitCode: 66)
    case RunError.invalidBundleArtifact(let path):
      self.init(
        code: "invalid_bundle_artifact", message: "invalid bundle artifact: \(path)", exitCode: 65)
    case RunError.provisioningDirectoryNotFound(let path):
      self.init(
        code: "missing_provisioning_directory",
        message: "provisioning directory not found: \(path)",
        exitCode: 66)
    case RunError.invalidConfiguration(let message):
      self.init(code: "invalid_configuration", message: message, exitCode: 69)
    case RunError.missingVirtioSocketDevice:
      self.init(
        code: "missing_virtio_socket", message: "run configuration has no Virtio socket device",
        exitCode: 69)
    case CloneError.unsupportedRequestSchema(let schema):
      self.init(
        code: "invalid_clone_request", message: "unsupported clone request schema: \(schema)",
        exitCode: 64)
    case CloneError.invalidRequest(let message):
      self.init(code: "invalid_clone_request", message: message, exitCode: 64)
    case CloneError.sourceBundleInvalid(let message):
      self.init(code: "invalid_source_bundle", message: message, exitCode: 65)
    case CloneError.destinationAlreadyExists(let path):
      self.init(
        code: "destination_exists", message: "destination bundle already exists: \(path)",
        exitCode: 73)
    case CloneError.crossDevice(let source, let destinationParent):
      self.init(
        code: "cross_device_clone",
        message:
          "APFS clone requires source \(source) and destination parent \(destinationParent) on the same device",
        exitCode: 73)
    case CloneError.cloneFileFailed(let source, let destination, let errorNumber):
      self.init(
        code: "clonefile_failed",
        message: "unable to APFS-clone \(source) to \(destination): errno \(errorNumber)",
        exitCode: 74)
    case CloneError.io(let message):
      self.init(code: "io_error", message: message, exitCode: 74)
    default:
      let nsError = error as NSError
      self.init(
        code: "operation_failed",
        message: "\(nsError.domain)(\(nsError.code)): \(nsError.localizedDescription)",
        exitCode: 70
      )
    }
  }

  private init(code: String, message: String, exitCode: Int32) {
    self.schemaVersion = 1
    self.code = code
    self.message = message
    self.exitCode = exitCode
  }
}
