import Darwin
import Foundation
import Virtualization

struct MacOSCloneService {
  private let fileManager: FileManager
  private let bundleFileSystem: InstallBundleFileSystem

  init(fileManager: FileManager = .default) {
    self.fileManager = fileManager
    self.bundleFileSystem = InstallBundleFileSystem(fileManager: fileManager)
  }

  @MainActor
  func clone(request: CloneRequest) throws -> CloneResultReport {
    try request.validateShape()
    let sourcePaths = InstallBundlePaths(root: request.sourceBundleURL)
    let destinationPaths = InstallBundlePaths(root: request.destinationBundleURL)
    try requireDirectoryWithoutSymlink(sourcePaths.root)
    let source = try validateSource(paths: sourcePaths)

    guard !fileManager.fileExists(atPath: destinationPaths.root.path) else {
      throw CloneError.destinationAlreadyExists(destinationPaths.root.path)
    }
    let destinationParent = destinationPaths.root.deletingLastPathComponent()
    try fileManager.createDirectory(
      at: destinationParent,
      withIntermediateDirectories: true,
      attributes: [.posixPermissions: 0o700])
    try requireGuardedDestinationParent(destinationParent)
    try requireSameDevice(source: sourcePaths.disk, destinationParent: destinationParent)
    try requireSameDevice(
      source: sourcePaths.auxiliaryStorage,
      destinationParent: destinationParent)

    do {
      try fileManager.createDirectory(
        at: destinationPaths.root,
        withIntermediateDirectories: false,
        attributes: [.posixPermissions: 0o700])
    } catch {
      if fileManager.fileExists(atPath: destinationPaths.root.path) {
        throw CloneError.destinationAlreadyExists(destinationPaths.root.path)
      }
      throw CloneError.io(error.localizedDescription)
    }

    var completed = false
    defer {
      if !completed {
        try? fileManager.removeItem(at: destinationPaths.root)
      }
    }

    try cloneInstalledArtifact(from: sourcePaths.disk, to: destinationPaths.disk)
    // A restored macOS installation stores required boot data in AuxiliaryStorage.
    try cloneInstalledArtifact(
      from: sourcePaths.auxiliaryStorage,
      to: destinationPaths.auxiliaryStorage)
    try bundleFileSystem.writeOpaqueData(
      source.hardwareModelData, to: destinationPaths.hardwareModel)

    let machineIdentifier = VZMacMachineIdentifier()
    let machineIdentifierData = machineIdentifier.dataRepresentation
    try bundleFileSystem.writeOpaqueData(
      machineIdentifierData, to: destinationPaths.machineIdentifier)
    try bundleFileSystem.writeOpaqueData(source.provenanceData, to: destinationPaths.provenance)

    let machineIdentifierDigest = InstallBundleFileSystem.sha256(machineIdentifierData)
    let auxiliaryMetadata = try RestoreImageService.fileMetadata(destinationPaths.auxiliaryStorage)
    guard auxiliaryMetadata.sha256 == source.auxiliaryStorageSHA256 else {
      throw CloneError.io("cloned AuxiliaryStorage digest does not match source")
    }
    let manifest = TemplateManifest(
      schemaVersion: 1,
      status: "installed",
      bundlePath: destinationPaths.root.path,
      guestOperatingSystemVersion: source.manifest.guestOperatingSystemVersion,
      guestBuildVersion: source.manifest.guestBuildVersion,
      architecture: source.manifest.architecture,
      ipswSHA256: source.manifest.ipswSHA256,
      hardwareModelSHA256: source.manifest.hardwareModelSHA256,
      templateMachineIdentifierSHA256: machineIdentifierDigest,
      rootDiskSizeBytes: source.manifest.rootDiskSizeBytes,
      cpuCount: source.manifest.cpuCount,
      memoryBytes: source.manifest.memoryBytes,
      deviceProfile: source.manifest.deviceProfile,
      failure: nil)
    try bundleFileSystem.writeJSON(manifest, to: destinationPaths.manifest)
    do {
      let configuration = try MacVirtualMachineConfiguration.make(
        hardwareModel: source.hardwareModel,
        machineIdentifier: machineIdentifier,
        auxiliaryStorage: VZMacAuxiliaryStorage(url: destinationPaths.auxiliaryStorage),
        rootDiskURL: destinationPaths.disk,
        cpuCount: manifest.cpuCount,
        memoryBytes: manifest.memoryBytes)
      try configuration.validate()
    } catch {
      throw CloneError.io(
        "destination VZ configuration validation failed: \(String(describing: error))")
    }

    completed = true
    return CloneResultReport(
      schemaVersion: 1,
      sourceBundlePath: sourcePaths.root.path,
      destinationBundlePath: destinationPaths.root.path,
      diskCloneMethod: "clonefile",
      diskSizeBytes: source.diskSizeBytes,
      hardwareModelSHA256: source.manifest.hardwareModelSHA256,
      machineIdentifierSHA256: machineIdentifierDigest,
      auxiliaryStorageSHA256: auxiliaryMetadata.sha256)
  }

  @MainActor
  private func validateSource(paths: InstallBundlePaths) throws -> ValidatedCloneSource {
    try requireRegularFileWithoutSymlink(paths.disk)
    try requireRegularFileWithoutSymlink(paths.auxiliaryStorage)
    try requireRegularFileWithoutSymlink(paths.hardwareModel)
    try requireRegularFileWithoutSymlink(paths.machineIdentifier)
    try requireRegularFileWithoutSymlink(paths.manifest)
    try requireRegularFileWithoutSymlink(paths.provenance)

    let manifest: TemplateManifest = try decodeJSON(at: paths.manifest)
    guard manifest.schemaVersion == 1, manifest.status == "installed", manifest.failure == nil
    else {
      throw CloneError.sourceBundleInvalid("manifest is not an installed schema-v1 bundle")
    }
    guard manifest.architecture == "arm64" else {
      throw CloneError.sourceBundleInvalid("manifest architecture must be arm64")
    }
    let hardwareModelData = try Data(contentsOf: paths.hardwareModel)
    guard let hardwareModel = VZMacHardwareModel(dataRepresentation: hardwareModelData) else {
      throw CloneError.sourceBundleInvalid("HardwareModel is not a VZMacHardwareModel")
    }
    guard hardwareModel.isSupported else {
      throw CloneError.sourceBundleInvalid("HardwareModel is unsupported on this host")
    }
    let hardwareDigest = InstallBundleFileSystem.sha256(hardwareModelData)
    guard hardwareDigest == manifest.hardwareModelSHA256 else {
      throw CloneError.sourceBundleInvalid("HardwareModel digest does not match manifest")
    }
    let machineIdentifierData = try Data(contentsOf: paths.machineIdentifier)
    guard
      let machineIdentifier = VZMacMachineIdentifier(
        dataRepresentation: machineIdentifierData)
    else {
      throw CloneError.sourceBundleInvalid("TemplateMachineIdentifier is invalid")
    }
    let machineDigest = InstallBundleFileSystem.sha256(machineIdentifierData)
    guard machineDigest == manifest.templateMachineIdentifierSHA256 else {
      throw CloneError.sourceBundleInvalid(
        "TemplateMachineIdentifier digest does not match manifest")
    }
    let provenanceData = try Data(contentsOf: paths.provenance)
    let provenance = try JSONDecoder().decode(TemplateProvenance.self, from: provenanceData)
    guard provenance.schemaVersion == 1,
      provenance.hardwareModelSHA256 == hardwareDigest,
      provenance.ipswSHA256 == manifest.ipswSHA256
    else {
      throw CloneError.sourceBundleInvalid("template provenance does not match manifest")
    }
    let diskSize = try paths.disk.resourceValues(forKeys: [.fileSizeKey]).fileSize
    guard let diskSize, UInt64(diskSize) == manifest.rootDiskSizeBytes else {
      throw CloneError.sourceBundleInvalid("Disk.img logical size does not match manifest")
    }
    let auxiliaryStorageSHA256 = try RestoreImageService.fileMetadata(paths.auxiliaryStorage).sha256
    do {
      let configuration = try MacVirtualMachineConfiguration.make(
        hardwareModel: hardwareModel,
        machineIdentifier: machineIdentifier,
        auxiliaryStorage: VZMacAuxiliaryStorage(url: paths.auxiliaryStorage),
        rootDiskURL: paths.disk,
        cpuCount: manifest.cpuCount,
        memoryBytes: manifest.memoryBytes)
      try configuration.validate()
    } catch {
      throw CloneError.sourceBundleInvalid(
        "VZ configuration validation failed: \(String(describing: error))")
    }
    return ValidatedCloneSource(
      hardwareModel: hardwareModel,
      hardwareModelData: hardwareModelData,
      manifest: manifest,
      provenanceData: provenanceData,
      diskSizeBytes: UInt64(diskSize),
      auxiliaryStorageSHA256: auxiliaryStorageSHA256)
  }

  private func decodeJSON<T: Decodable>(at url: URL) throws -> T {
    do {
      return try JSONDecoder().decode(T.self, from: Data(contentsOf: url))
    } catch let error as CloneError {
      throw error
    } catch {
      throw CloneError.sourceBundleInvalid(
        "\(url.lastPathComponent): \(error.localizedDescription)")
    }
  }

  private func requireDirectoryWithoutSymlink(_ url: URL) throws {
    var metadata = stat()
    guard lstat(url.path, &metadata) == 0,
      metadata.st_mode & S_IFMT == S_IFDIR
    else {
      throw CloneError.sourceBundleInvalid("bundle path is not a directory or is a symlink")
    }
  }

  private func requireRegularFileWithoutSymlink(_ url: URL) throws {
    var metadata = stat()
    guard lstat(url.path, &metadata) == 0,
      metadata.st_mode & S_IFMT == S_IFREG
    else {
      throw CloneError.sourceBundleInvalid(
        "\(url.lastPathComponent) is missing, not regular, or a symlink")
    }
  }

  private func requireGuardedDestinationParent(_ url: URL) throws {
    var metadata = stat()
    guard lstat(url.path, &metadata) == 0,
      metadata.st_mode & S_IFMT == S_IFDIR,
      metadata.st_uid == getuid(),
      metadata.st_mode & 0o022 == 0
    else {
      throw CloneError.invalidRequest(
        "destination parent must be a same-user directory without group/other write access")
    }
  }

  private func requireSameDevice(source: URL, destinationParent: URL) throws {
    var sourceMetadata = stat()
    var destinationMetadata = stat()
    guard stat(source.path, &sourceMetadata) == 0 else {
      throw CloneError.io("stat \(source.path): \(String(cString: strerror(errno)))")
    }
    guard stat(destinationParent.path, &destinationMetadata) == 0 else {
      throw CloneError.io("stat \(destinationParent.path): \(String(cString: strerror(errno)))")
    }
    guard sourceMetadata.st_dev == destinationMetadata.st_dev else {
      throw CloneError.crossDevice(
        source: source.path,
        destinationParent: destinationParent.path)
    }
  }

  private func cloneInstalledArtifact(from source: URL, to destination: URL) throws {
    let flags = UInt32(CLONE_NOFOLLOW_ANY | CLONE_NOOWNERCOPY)
    guard clonefile(source.path, destination.path, flags) == 0 else {
      throw CloneError.cloneFileFailed(
        source: source.path,
        destination: destination.path,
        errno: errno)
    }
    do {
      try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
    } catch {
      throw CloneError.io(error.localizedDescription)
    }
  }
}

private struct ValidatedCloneSource {
  let hardwareModel: VZMacHardwareModel
  let hardwareModelData: Data
  let manifest: TemplateManifest
  let provenanceData: Data
  let diskSizeBytes: UInt64
  let auxiliaryStorageSHA256: String
}
