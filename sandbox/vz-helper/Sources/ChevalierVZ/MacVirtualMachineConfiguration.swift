import Foundation
import Virtualization

struct MacVirtualMachineConfiguration {
  static let deviceProfile = "macos-v1-root-graphics-input-entropy-vsock-no-network"

  @MainActor
  static func make(
    hardwareModel: VZMacHardwareModel,
    machineIdentifier: VZMacMachineIdentifier,
    auxiliaryStorage: VZMacAuxiliaryStorage,
    rootDiskURL: URL,
    cpuCount: Int,
    memoryBytes: UInt64
  ) throws -> VZVirtualMachineConfiguration {
    let platform = VZMacPlatformConfiguration()
    platform.hardwareModel = hardwareModel
    platform.machineIdentifier = machineIdentifier
    platform.auxiliaryStorage = auxiliaryStorage

    let attachment = try VZDiskImageStorageDeviceAttachment(
      url: rootDiskURL,
      readOnly: false,
      cachingMode: .automatic,
      synchronizationMode: .full)
    let rootDisk = VZVirtioBlockDeviceConfiguration(attachment: attachment)
    rootDisk.blockDeviceIdentifier = "root"

    let graphics = VZMacGraphicsDeviceConfiguration()
    graphics.displays = [
      VZMacGraphicsDisplayConfiguration(
        widthInPixels: 1920,
        heightInPixels: 1080,
        pixelsPerInch: 80)
    ]

    let configuration = VZVirtualMachineConfiguration()
    configuration.bootLoader = VZMacOSBootLoader()
    configuration.platform = platform
    configuration.cpuCount = cpuCount
    configuration.memorySize = memoryBytes
    configuration.storageDevices = [rootDisk]
    configuration.graphicsDevices = [graphics]
    configuration.keyboards = [VZMacKeyboardConfiguration()]
    configuration.pointingDevices = [VZMacTrackpadConfiguration()]
    configuration.entropyDevices = [VZVirtioEntropyDeviceConfiguration()]
    configuration.socketDevices = [VZVirtioSocketDeviceConfiguration()]
    configuration.networkDevices = []
    configuration.audioDevices = []
    configuration.directorySharingDevices = []

    do {
      try configuration.validate()
    } catch {
      throw InstallError.invalidConfiguration(error.localizedDescription)
    }
    return configuration
  }
}
