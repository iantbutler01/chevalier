import Foundation
import Virtualization

struct LoadedMacOSInstallBundle {
  let hardwareModel: VZMacHardwareModel
  let machineIdentifier: VZMacMachineIdentifier
  let auxiliaryStorage: VZMacAuxiliaryStorage
  let rootDiskURL: URL
}

struct MacOSRunConfiguration {
  private let fileManager: FileManager

  init(fileManager: FileManager = .default) {
    self.fileManager = fileManager
  }

  func loadBundle(at bundleURL: URL) throws -> LoadedMacOSInstallBundle {
    let paths = InstallBundlePaths(root: bundleURL)
    try requireRegularFile(paths.disk)
    try requireRegularFile(paths.auxiliaryStorage)
    try requireRegularFile(paths.hardwareModel)
    try requireRegularFile(paths.machineIdentifier)

    let hardwareModelData = try Data(contentsOf: paths.hardwareModel)
    guard let hardwareModel = VZMacHardwareModel(dataRepresentation: hardwareModelData) else {
      throw RunError.invalidBundleArtifact(paths.hardwareModel.path)
    }
    let machineIdentifierData = try Data(contentsOf: paths.machineIdentifier)
    guard
      let machineIdentifier = VZMacMachineIdentifier(
        dataRepresentation: machineIdentifierData)
    else {
      throw RunError.invalidBundleArtifact(paths.machineIdentifier.path)
    }

    return LoadedMacOSInstallBundle(
      hardwareModel: hardwareModel,
      machineIdentifier: machineIdentifier,
      auxiliaryStorage: VZMacAuxiliaryStorage(url: paths.auxiliaryStorage),
      rootDiskURL: paths.disk)
  }

  @MainActor
  func make(request: RunRequest) throws -> VZVirtualMachineConfiguration {
    try request.validateShape()
    let bundle = try loadBundle(at: request.bundleURL)
    let configuration = try MacVirtualMachineConfiguration.make(
      hardwareModel: bundle.hardwareModel,
      machineIdentifier: bundle.machineIdentifier,
      auxiliaryStorage: bundle.auxiliaryStorage,
      rootDiskURL: bundle.rootDiskURL,
      cpuCount: request.cpuCount,
      memoryBytes: request.memoryBytes)

    var directorySharingDevices: [VZDirectorySharingDeviceConfiguration] = []
    if let provisioningDirectoryURL = request.provisioningDirectoryURL {
      try requireDirectory(provisioningDirectoryURL)
      let sharedDirectory = VZSharedDirectory(
        url: provisioningDirectoryURL,
        readOnly: request.provisioningDirectoryReadOnly ?? true)
      let fileSystem = VZVirtioFileSystemDeviceConfiguration(
        tag: VZVirtioFileSystemDeviceConfiguration.macOSGuestAutomountTag)
      fileSystem.share = VZSingleDirectoryShare(directory: sharedDirectory)
      directorySharingDevices.append(fileSystem)
    }

    if let sharedDirectories = request.sharedDirectories, !sharedDirectories.isEmpty {
      var directories: [String: VZSharedDirectory] = [:]
      for directory in sharedDirectories {
        try requireSharedDirectory(directory.hostURL)
        do {
          try VZMultipleDirectoryShare.validateName(directory.name)
        } catch {
          throw RunError.invalidRequest(
            "invalid sharedDirectories name \(directory.name): \(error.localizedDescription)")
        }
        directories[directory.name] = VZSharedDirectory(
          url: directory.hostURL,
          readOnly: directory.readOnly)
      }
      let fileSystem = VZVirtioFileSystemDeviceConfiguration(
        tag: VZVirtioFileSystemDeviceConfiguration.macOSGuestAutomountTag)
      fileSystem.share = VZMultipleDirectoryShare(directories: directories)
      directorySharingDevices.append(fileSystem)
    }
    configuration.directorySharingDevices = directorySharingDevices

    switch request.networkMode ?? .none {
    case .none:
      configuration.networkDevices = []
    case .natDevelopment:
      let network = VZVirtioNetworkDeviceConfiguration()
      network.attachment = VZNATNetworkDeviceAttachment()
      configuration.networkDevices = [network]
    }
    do {
      try configuration.validate()
    } catch {
      throw RunError.invalidConfiguration(error.localizedDescription)
    }
    return configuration
  }

  private func requireRegularFile(_ url: URL) throws {
    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory),
      !isDirectory.boolValue
    else {
      throw RunError.missingBundleArtifact(url.path)
    }
  }

  private func requireDirectory(_ url: URL) throws {
    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory), isDirectory.boolValue
    else {
      throw RunError.provisioningDirectoryNotFound(url.path)
    }
  }

  private func requireSharedDirectory(_ url: URL) throws {
    var isDirectory: ObjCBool = false
    guard fileManager.fileExists(atPath: url.path, isDirectory: &isDirectory), isDirectory.boolValue
    else {
      throw RunError.sharedDirectoryNotFound(url.path)
    }
  }
}
