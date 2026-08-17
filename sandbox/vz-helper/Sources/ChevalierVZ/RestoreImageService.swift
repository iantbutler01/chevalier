import CryptoKit
import Foundation
import Security
import Virtualization

enum HelperError: Error, Equatable {
  case notARegularFile(String)
  case unsupportedHostArchitecture
  case checksumMismatch(expected: String, actual: String)
  case unsupportedRestoreImage(version: String, build: String)
  case missingConfigurationRequirements(version: String, build: String)
}

struct RestoreImageService {
  func probe() async throws -> ProbeReport {
    try requireArm64()
    let restoreImage = try await VZMacOSRestoreImage.latestSupported
    return ProbeReport(
      schemaVersion: VersionReport.current.schemaVersion,
      helperVersion: VersionReport.current.helperVersion,
      protocolVersion: VersionReport.current.protocolVersion,
      host: hostReport(),
      latestSupportedRestoreImage: restoreImageReport(restoreImage, source: "latest_supported")
    )
  }

  func inspectImage(at url: URL, expectedSHA256: String? = nil) async throws -> RestoreImageReport {
    try requireArm64()
    guard isRegularFile(at: url) else {
      throw HelperError.notARegularFile(url.path)
    }
    let fileMetadata = try Self.fileMetadata(url)
    if let expectedSHA256, fileMetadata.sha256 != expectedSHA256 {
      throw HelperError.checksumMismatch(expected: expectedSHA256, actual: fileMetadata.sha256)
    }
    let restoreImage = try await VZMacOSRestoreImage.image(from: url)
    let version = Self.versionString(restoreImage.operatingSystemVersion)
    guard restoreImage.isSupported else {
      throw HelperError.unsupportedRestoreImage(version: version, build: restoreImage.buildVersion)
    }
    guard restoreImage.mostFeaturefulSupportedConfiguration != nil else {
      throw HelperError.missingConfigurationRequirements(
        version: version, build: restoreImage.buildVersion)
    }
    return restoreImageReport(restoreImage, source: "local", fileMetadata: fileMetadata)
  }

  private func restoreImageReport(
    _ restoreImage: VZMacOSRestoreImage,
    source: String,
    fileMetadata: (size: UInt64, sha256: String)? = nil
  ) -> RestoreImageReport {
    RestoreImageReport(
      schemaVersion: 1,
      source: source,
      url: restoreImage.url.absoluteString,
      isLocalFile: restoreImage.url.isFileURL,
      operatingSystemVersion: Self.versionString(restoreImage.operatingSystemVersion),
      buildVersion: restoreImage.buildVersion,
      supported: restoreImage.isSupported,
      configurationRequirements: restoreImage.mostFeaturefulSupportedConfiguration.map {
        ConfigurationRequirementsReport(
          minimumSupportedCPUCount: $0.minimumSupportedCPUCount,
          minimumSupportedMemoryBytes: $0.minimumSupportedMemorySize,
          hardwareModelSupported: $0.hardwareModel.isSupported,
          hardwareModelSHA256: Self.sha256($0.hardwareModel.dataRepresentation)
        )
      },
      fileSizeBytes: fileMetadata?.size,
      sha256: fileMetadata?.sha256
    )
  }

  private func hostReport() -> HostReport {
    let processInfo = ProcessInfo.processInfo
    return HostReport(
      architecture: Self.hostArchitecture,
      operatingSystemVersion: Self.versionString(processInfo.operatingSystemVersion),
      operatingSystemBuild: Self.operatingSystemBuild,
      virtualizationSupported: VZVirtualMachine.isSupported,
      virtualizationEntitlementPresent: Self.virtualizationEntitlementPresent,
      minimumAllowedCPUCount: VZVirtualMachineConfiguration.minimumAllowedCPUCount,
      maximumAllowedCPUCount: VZVirtualMachineConfiguration.maximumAllowedCPUCount,
      minimumAllowedMemoryBytes: VZVirtualMachineConfiguration.minimumAllowedMemorySize,
      maximumAllowedMemoryBytes: VZVirtualMachineConfiguration.maximumAllowedMemorySize
    )
  }

  private func isRegularFile(at url: URL) -> Bool {
    guard url.isFileURL else {
      return false
    }
    let values = try? url.resourceValues(forKeys: [.isRegularFileKey])
    return values?.isRegularFile == true
  }

  private func requireArm64() throws {
    guard Self.hostArchitecture == "arm64" else {
      throw HelperError.unsupportedHostArchitecture
    }
  }

  static func versionString(_ version: OperatingSystemVersion) -> String {
    "\(version.majorVersion).\(version.minorVersion).\(version.patchVersion)"
  }

  static func fileMetadata(_ url: URL) throws -> (size: UInt64, sha256: String) {
    let values = try url.resourceValues(forKeys: [.fileSizeKey])
    let fileHandle = try FileHandle(forReadingFrom: url)
    defer { try? fileHandle.close() }

    var hasher = SHA256()
    while let data = try fileHandle.read(upToCount: 4 * 1024 * 1024), !data.isEmpty {
      hasher.update(data: data)
    }

    let digest = hasher.finalize().map { String(format: "%02x", $0) }.joined()
    return (UInt64(values.fileSize ?? 0), digest)
  }

  private static func sha256(_ data: Data) -> String {
    SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
  }

  private static var hostArchitecture: String {
    #if arch(arm64)
      "arm64"
    #elseif arch(x86_64)
      "x86_64"
    #else
      "unknown"
    #endif
  }

  private static var operatingSystemBuild: String {
    let url = URL(fileURLWithPath: "/System/Library/CoreServices/SystemVersion.plist")
    guard
      let data = try? Data(contentsOf: url),
      let propertyList = try? PropertyListSerialization.propertyList(from: data, format: nil),
      let dictionary = propertyList as? [String: Any],
      let build = dictionary["ProductBuildVersion"] as? String
    else {
      return "unknown"
    }
    return build
  }

  private static var virtualizationEntitlementPresent: Bool {
    guard let task = SecTaskCreateFromSelf(nil) else {
      return false
    }
    let entitlement = "com.apple.security.virtualization" as CFString
    return SecTaskCopyValueForEntitlement(task, entitlement, nil) as? Bool == true
  }
}
