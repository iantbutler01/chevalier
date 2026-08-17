import CryptoKit
import Darwin
import Foundation

struct InstallBundlePaths: Equatable {
  let root: URL

  var disk: URL { root.appendingPathComponent("Disk.img") }
  var auxiliaryStorage: URL { root.appendingPathComponent("AuxiliaryStorage") }
  var hardwareModel: URL { root.appendingPathComponent("HardwareModel") }
  var machineIdentifier: URL { root.appendingPathComponent("TemplateMachineIdentifier") }
  var manifest: URL { root.appendingPathComponent("manifest.json") }
  var provenance: URL { root.appendingPathComponent("template-provenance.json") }
}

struct InstallBundleFileSystem {
  private let fileManager: FileManager

  init(fileManager: FileManager = .default) {
    self.fileManager = fileManager
  }

  func prepareTarget(_ bundleURL: URL, requiredFreeSpaceBytes: UInt64) throws -> InstallBundlePaths
  {
    guard !fileManager.fileExists(atPath: bundleURL.path) else {
      throw InstallError.bundleAlreadyExists(bundleURL.path)
    }

    let parent = bundleURL.deletingLastPathComponent()
    try fileManager.createDirectory(
      at: parent, withIntermediateDirectories: true,
      attributes: [.posixPermissions: 0o700])
    let available = try availableCapacity(at: parent)
    guard available >= requiredFreeSpaceBytes else {
      throw InstallError.insufficientFreeSpace(
        path: parent.path, required: requiredFreeSpaceBytes, available: available)
    }

    try fileManager.createDirectory(
      at: bundleURL, withIntermediateDirectories: false,
      attributes: [.posixPermissions: 0o700])
    return InstallBundlePaths(root: bundleURL)
  }

  func createSparseDisk(at url: URL, size: UInt64) throws {
    let descriptor = open(url.path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR)
    guard descriptor >= 0 else {
      throw InstallError.unableToCreateSparseDisk(path: url.path, errno: errno)
    }
    defer { close(descriptor) }
    guard size <= UInt64(Int64.max), ftruncate(descriptor, off_t(size)) == 0 else {
      throw InstallError.unableToCreateSparseDisk(path: url.path, errno: errno)
    }
  }

  func writeOpaqueData(_ data: Data, to url: URL) throws {
    try data.write(to: url, options: [.atomic])
    try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
  }

  func writeJSON<T: Encodable>(_ value: T, to url: URL) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    var data = try encoder.encode(value)
    data.append(0x0a)
    try writeOpaqueData(data, to: url)
  }

  func availableCapacity(at url: URL) throws -> UInt64 {
    let values = try url.resourceValues(forKeys: [.volumeAvailableCapacityKey])
    if let capacity = values.volumeAvailableCapacity, capacity >= 0 {
      return UInt64(capacity)
    }
    let attributes = try fileManager.attributesOfFileSystem(forPath: url.path)
    return (attributes[.systemFreeSize] as? NSNumber)?.uint64Value ?? 0
  }

  static func sha256(_ data: Data) -> String {
    SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
  }
}
