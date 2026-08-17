import Foundation
import Virtualization

struct MacOSInstallService {
  private let fileSystem: InstallBundleFileSystem

  init(fileSystem: InstallBundleFileSystem = InstallBundleFileSystem()) {
    self.fileSystem = fileSystem
  }

  @MainActor
  func install(request: InstallRequest, eventsURL: URL) async throws -> InstallResultReport {
    try request.validateShape()
    let events = try InstallEventWriter(url: eventsURL)
    events.emit(phase: "preflight", fraction: 0, message: "validating restore image and host")

    let ipswURL = request.ipswURL
    let values = try ipswURL.resourceValues(forKeys: [.isRegularFileKey])
    guard values.isRegularFile == true else {
      throw InstallError.restoreImageNotRegularFile(ipswURL.path)
    }
    let metadata = try RestoreImageService.fileMetadata(ipswURL)
    let expectedDigest = request.expectedSHA256.lowercased()
    guard metadata.sha256 == expectedDigest else {
      throw InstallError.restoreImageChecksumMismatch(
        expected: expectedDigest, actual: metadata.sha256)
    }

    let restoreImage = try await VZMacOSRestoreImage.image(from: ipswURL)
    let version = RestoreImageService.versionString(restoreImage.operatingSystemVersion)
    guard restoreImage.isSupported else {
      throw InstallError.unsupportedRestoreImage(version: version, build: restoreImage.buildVersion)
    }
    guard let requirements = restoreImage.mostFeaturefulSupportedConfiguration else {
      throw InstallError.missingConfigurationRequirements(
        version: version, build: restoreImage.buildVersion)
    }

    try validateResources(request: request, requirements: requirements)
    let paths = try fileSystem.prepareTarget(
      request.bundleURL, requiredFreeSpaceBytes: request.requiredFreeSpaceBytes)
    events.emit(phase: "bundle", fraction: 0, message: "creating guarded sparse bundle")

    let hardwareModel = requirements.hardwareModel
    let machineIdentifier = VZMacMachineIdentifier()
    let hardwareModelData = hardwareModel.dataRepresentation
    let machineIdentifierData = machineIdentifier.dataRepresentation
    let hardwareModelDigest = InstallBundleFileSystem.sha256(hardwareModelData)
    let machineIdentifierDigest = InstallBundleFileSystem.sha256(machineIdentifierData)

    try fileSystem.createSparseDisk(at: paths.disk, size: request.rootDiskSizeBytes)
    try fileSystem.writeOpaqueData(hardwareModelData, to: paths.hardwareModel)
    try fileSystem.writeOpaqueData(machineIdentifierData, to: paths.machineIdentifier)
    let auxiliaryStorage = try VZMacAuxiliaryStorage(
      creatingStorageAt: paths.auxiliaryStorage,
      hardwareModel: hardwareModel,
      options: [])
    try FileManager.default.setAttributes(
      [.posixPermissions: 0o600], ofItemAtPath: paths.auxiliaryStorage.path)

    let provenance = TemplateProvenance(
      schemaVersion: 1,
      ipswPath: ipswURL.path,
      ipswSizeBytes: metadata.size,
      ipswSHA256: metadata.sha256,
      guestOperatingSystemVersion: version,
      guestBuildVersion: restoreImage.buildVersion,
      hardwareModelSHA256: hardwareModelDigest,
      createdAt: Date().ISO8601Format())
    try fileSystem.writeJSON(provenance, to: paths.provenance)
    try writeManifest(
      status: "installing", failure: nil, request: request, paths: paths,
      version: version, build: restoreImage.buildVersion, ipswDigest: metadata.sha256,
      hardwareModelDigest: hardwareModelDigest, machineIdentifierDigest: machineIdentifierDigest)

    let configuration = try MacVirtualMachineConfiguration.make(
      hardwareModel: hardwareModel,
      machineIdentifier: machineIdentifier,
      auxiliaryStorage: auxiliaryStorage,
      rootDiskURL: paths.disk,
      cpuCount: request.cpuCount,
      memoryBytes: request.memoryBytes)
    let saveRestoreValidation = validateSaveRestore(configuration)
    let virtualMachine = VZVirtualMachine(configuration: configuration)
    let installer = VZMacOSInstaller(
      virtualMachine: virtualMachine,
      restoringFromImageAt: ipswURL)
    let observation = installer.progress.observe(\.fractionCompleted, options: [.initial, .new]) {
      progress, _ in
      events.emit(phase: "installing", fraction: progress.fractionCompleted)
    }
    defer { observation.invalidate() }

    do {
      try await installer.install()
      try events.throwIfFailed()
      events.emit(
        phase: "complete", fraction: 1, message: "macOS installation completed", force: true)
      try events.throwIfFailed()
      try writeManifest(
        status: "installed", failure: nil, request: request, paths: paths,
        version: version, build: restoreImage.buildVersion, ipswDigest: metadata.sha256,
        hardwareModelDigest: hardwareModelDigest,
        machineIdentifierDigest: machineIdentifierDigest)
    } catch {
      let failure = (error as NSError).localizedDescription
      try? writeManifest(
        status: "failed", failure: failure, request: request, paths: paths,
        version: version, build: restoreImage.buildVersion, ipswDigest: metadata.sha256,
        hardwareModelDigest: hardwareModelDigest,
        machineIdentifierDigest: machineIdentifierDigest)
      events.emit(
        phase: "failed", fraction: installer.progress.fractionCompleted, message: failure,
        force: true)
      throw error
    }

    return InstallResultReport(
      schemaVersion: 1,
      bundlePath: paths.root.path,
      guestOperatingSystemVersion: version,
      guestBuildVersion: restoreImage.buildVersion,
      ipswSHA256: metadata.sha256,
      hardwareModelSHA256: hardwareModelDigest,
      machineIdentifierSHA256: machineIdentifierDigest,
      rootDiskSizeBytes: request.rootDiskSizeBytes,
      cpuCount: request.cpuCount,
      memoryBytes: request.memoryBytes,
      saveRestoreSupported: saveRestoreValidation.supported,
      saveRestoreValidationError: saveRestoreValidation.error)
  }

  private func validateResources(
    request: InstallRequest,
    requirements: VZMacOSConfigurationRequirements
  ) throws {
    let minimumCPU = max(
      requirements.minimumSupportedCPUCount,
      VZVirtualMachineConfiguration.minimumAllowedCPUCount)
    let maximumCPU = VZVirtualMachineConfiguration.maximumAllowedCPUCount
    guard request.cpuCount >= minimumCPU, request.cpuCount <= maximumCPU else {
      throw InstallError.resourceOutsideAllowedRange(
        resource: "cpuCount", requested: UInt64(request.cpuCount),
        minimum: UInt64(minimumCPU), maximum: UInt64(maximumCPU))
    }

    let minimumMemory = max(
      requirements.minimumSupportedMemorySize,
      VZVirtualMachineConfiguration.minimumAllowedMemorySize)
    let maximumMemory = VZVirtualMachineConfiguration.maximumAllowedMemorySize
    guard request.memoryBytes >= minimumMemory, request.memoryBytes <= maximumMemory else {
      throw InstallError.resourceOutsideAllowedRange(
        resource: "memoryBytes", requested: request.memoryBytes,
        minimum: minimumMemory, maximum: maximumMemory)
    }
  }

  @MainActor
  private func validateSaveRestore(
    _ configuration: VZVirtualMachineConfiguration
  ) -> (supported: Bool, error: String?) {
    do {
      try configuration.validateSaveRestoreSupport()
      return (true, nil)
    } catch {
      return (false, (error as NSError).localizedDescription)
    }
  }

  private func writeManifest(
    status: String,
    failure: String?,
    request: InstallRequest,
    paths: InstallBundlePaths,
    version: String,
    build: String,
    ipswDigest: String,
    hardwareModelDigest: String,
    machineIdentifierDigest: String
  ) throws {
    try fileSystem.writeJSON(
      TemplateManifest(
        schemaVersion: 1,
        status: status,
        bundlePath: paths.root.path,
        guestOperatingSystemVersion: version,
        guestBuildVersion: build,
        architecture: "arm64",
        ipswSHA256: ipswDigest,
        hardwareModelSHA256: hardwareModelDigest,
        templateMachineIdentifierSHA256: machineIdentifierDigest,
        rootDiskSizeBytes: request.rootDiskSizeBytes,
        cpuCount: request.cpuCount,
        memoryBytes: request.memoryBytes,
        deviceProfile: MacVirtualMachineConfiguration.deviceProfile,
        failure: failure),
      to: paths.manifest)
  }
}
