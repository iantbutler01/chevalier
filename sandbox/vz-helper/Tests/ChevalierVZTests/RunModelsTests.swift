import Darwin
import Foundation
import Virtualization

#if !VALIDATE_RUNTIME
  import Testing

  @testable import ChevalierVZ
#endif

#if VALIDATE_RUNTIME
  @main
  struct RuntimeConfigurationValidation {
    @MainActor
    static func main() throws {
      let environment = ProcessInfo.processInfo.environment
      guard let bundlePath = environment["CHEVALIER_VZ_TEST_BUNDLE_PATH"] else {
        throw RunError.invalidRequest("CHEVALIER_VZ_TEST_BUNDLE_PATH is required")
      }
      let request = RunRequest(
        schemaVersion: 1,
        bundlePath: bundlePath,
        cpuCount: 4,
        memoryBytes: 4 * 1024 * 1024 * 1024,
        provisioningDirectoryPath: environment["CHEVALIER_VZ_TEST_PROVISIONING_PATH"],
        provisioningDirectoryReadOnly: true,
        networkMode: nil,
        viewerMode: nil,
        loopbackRelayPort: nil,
        guestServiceRelays: nil)
      let configuration = try MacOSRunConfiguration().make(request: request)
      precondition(configuration.networkDevices.isEmpty)
      precondition(configuration.storageDevices.count == 1)
      precondition(configuration.socketDevices.count == 1)
      precondition(configuration.directorySharingDevices.count <= 1)
      print(
        "validated storage=\(configuration.storageDevices.count) network=\(configuration.networkDevices.count) socket=\(configuration.socketDevices.count) shares=\(configuration.directorySharingDevices.count)"
      )
    }
  }
#else
  @Test func validatesRunRequestShapeAndPaths() throws {
    let request = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 8 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: "/tmp/provisioning",
      provisioningDirectoryReadOnly: true,
      networkMode: .natDevelopment,
      viewerMode: .window,
      loopbackRelayPort: 13_338,
      guestIngressRelayPort: 13_337,
      guestServiceRelays: [
        GuestServiceRelayRequest(vsockPort: 13_339, hostLoopbackPort: 63_339)
      ])

    try request.validateShape()
    #expect(request.bundleURL.path == "/tmp/template.bundle")
    #expect(request.provisioningDirectoryURL?.path == "/tmp/provisioning")
    #expect(request.networkMode == .natDevelopment)
    #expect(request.effectiveViewerMode == .window)
    #expect(request.loopbackRelayPort == 13_338)
    #expect(request.guestIngressRelayPort == 13_337)
    #expect(request.guestServiceRelays?.first?.vsockPort == 13_339)
  }

  @Test func rejectsInvalidRunRequestResources() {
    let request = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 0,
      memoryBytes: 8 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil)

    #expect(throws: RunError.invalidRequest("cpuCount must be greater than zero")) {
      try request.validateShape()
    }
  }

  @Test func bundleLoaderNamesMissingPersistedArtifact() throws {
    let directory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }

    #expect(
      throws: RunError.missingBundleArtifact(directory.appendingPathComponent("Disk.img").path)
    ) {
      try MacOSRunConfiguration().loadBundle(at: directory)
    }
  }

  @Test func guestControlSocketUsesProtocolPort() {
    #expect(GuestControlSocketListener.port == 13_338)
    #expect(GuestControlSocketListener().connectionCount == 0)
  }

  @Test func rejectsZeroLoopbackRelayPort() {
    let request = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: 0,
      guestServiceRelays: nil)

    #expect(
      throws: RunError.invalidRequest(
        "loopbackRelayPort must be greater than zero when present")
    ) {
      try request.validateShape()
    }
  }

  @Test func runRequestRemainsBackwardCompatibleWithoutRelayField() throws {
    let data = Data(
      """
      {
        "schemaVersion": 1,
        "bundlePath": "/tmp/template.bundle",
        "cpuCount": 4,
        "memoryBytes": 4294967296,
        "provisioningDirectoryPath": null,
        "provisioningDirectoryReadOnly": null
      }
      """.utf8)

    let request = try JSONDecoder().decode(RunRequest.self, from: data)
    #expect(request.loopbackRelayPort == nil)
    #expect(request.guestIngressRelayPort == nil)
    #expect(request.guestServiceRelays == nil)
    #expect(request.networkMode == nil)
    #expect(request.viewerMode == nil)
    #expect(request.effectiveViewerMode == .headless)
    #expect(request.ownerControlSocketPath == nil)
    #expect(request.runtimeGeneration == nil)
  }

  @Test func decodesExplicitViewerModesAndRejectsUnknownValues() throws {
    func requestData(viewerMode: String) -> Data {
      Data(
        """
        {
          "schemaVersion": 1,
          "bundlePath": "/tmp/template.bundle",
          "cpuCount": 4,
          "memoryBytes": 4294967296,
          "viewerMode": "\(viewerMode)"
        }
        """.utf8)
    }

    let headless = try JSONDecoder().decode(
      RunRequest.self,
      from: requestData(viewerMode: "headless"))
    let window = try JSONDecoder().decode(
      RunRequest.self,
      from: requestData(viewerMode: "window"))

    #expect(headless.effectiveViewerMode == .headless)
    #expect(window.effectiveViewerMode == .window)
    #expect(throws: DecodingError.self) {
      try JSONDecoder().decode(
        RunRequest.self,
        from: requestData(viewerMode: "unknown"))
    }
  }

  @Test func rejectsInvalidGuestServiceRelayPorts() {
    let zeroVSock = makeRunRequest(
      guestServiceRelays: [
        GuestServiceRelayRequest(vsockPort: 0, hostLoopbackPort: 63_339)
      ])
    #expect(
      throws: RunError.invalidRequest(
        "guestServiceRelays vsockPort must be greater than zero")
    ) {
      try zeroVSock.validateShape()
    }

    let zeroHost = makeRunRequest(
      guestServiceRelays: [
        GuestServiceRelayRequest(vsockPort: 13_339, hostLoopbackPort: 0)
      ])
    #expect(
      throws: RunError.invalidRequest(
        "guestServiceRelays hostLoopbackPort must be greater than zero")
    ) {
      try zeroHost.validateShape()
    }
  }

  @Test func rejectsInvalidGuestIngressRelayPorts() {
    let zero = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestIngressRelayPort: 0,
      guestServiceRelays: nil)
    #expect(
      throws: RunError.invalidRequest(
        "guestIngressRelayPort must be greater than zero when present")
    ) {
      try zero.validateShape()
    }

    let collision = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: 13_338,
      guestIngressRelayPort: 13_338,
      guestServiceRelays: nil)
    #expect(
      throws: RunError.invalidRequest(
        "guestIngressRelayPort must not collide with loopbackRelayPort")
    ) {
      try collision.validateShape()
    }
  }

  @Test func rejectsGuestServiceRelayControlCollisionAndDuplicates() {
    let controlCollision = makeRunRequest(
      guestServiceRelays: [
        GuestServiceRelayRequest(
          vsockPort: RunRequest.controlVSockPort,
          hostLoopbackPort: 63_339)
      ])
    #expect(
      throws: RunError.invalidRequest(
        "guestServiceRelays vsockPort must not collide with control port 13338")
    ) {
      try controlCollision.validateShape()
    }

    let duplicate = makeRunRequest(
      guestServiceRelays: [
        GuestServiceRelayRequest(vsockPort: 13_339, hostLoopbackPort: 63_339),
        GuestServiceRelayRequest(vsockPort: 13_339, hostLoopbackPort: 63_340),
      ])
    #expect(
      throws: RunError.invalidRequest(
        "guestServiceRelays contains duplicate vsockPort 13339")
    ) {
      try duplicate.validateShape()
    }
  }

  @Test func validatesSharedDirectoryShapeAndDuplicateNames() throws {
    let request = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil,
      sharedDirectories: [
        SharedDirectoryRequest(hostPath: "/tmp/workspace", name: "workspace", readOnly: false)
      ])
    try request.validateShape()

    let duplicate = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil,
      sharedDirectories: [
        SharedDirectoryRequest(hostPath: "/tmp/one", name: "workspace", readOnly: false),
        SharedDirectoryRequest(hostPath: "/tmp/two", name: "workspace", readOnly: true),
      ])
    #expect(
      throws: RunError.invalidRequest("sharedDirectories contains duplicate name workspace")
    ) {
      try duplicate.validateShape()
    }

    let provisioningConflict = RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: "/tmp/provisioning",
      provisioningDirectoryReadOnly: true,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil,
      sharedDirectories: [
        SharedDirectoryRequest(hostPath: "/tmp/workspace", name: "workspace", readOnly: false)
      ])
    #expect(
      throws: RunError.invalidRequest(
        "provisioningDirectoryPath and sharedDirectories cannot be used together")
    ) {
      try provisioningConflict.validateShape()
    }
  }

  @Test func guestServiceRelayEnforcesConcurrentSessionCap() {
    let limit = ConcurrentSessionLimit(maximum: GuestServiceRelay.maximumConcurrentSessions)
    for _ in 0..<GuestServiceRelay.maximumConcurrentSessions {
      #expect(limit.acquire())
    }
    #expect(!limit.acquire())
    #expect(limit.activeCount == GuestServiceRelay.maximumConcurrentSessions)
    limit.release()
    #expect(limit.acquire())
  }

  @Test func relaysFileDescriptorsBidirectionally() throws {
    var left = [Int32](repeating: 0, count: 2)
    var right = [Int32](repeating: 0, count: 2)
    #expect(socketpair(AF_UNIX, SOCK_STREAM, 0, &left) == 0)
    #expect(socketpair(AF_UNIX, SOCK_STREAM, 0, &right) == 0)
    defer {
      for descriptor in left {
        Darwin.close(descriptor)
      }
      for descriptor in right {
        Darwin.close(descriptor)
      }
    }

    let relayLeft = left[0]
    let relayRight = right[0]
    let completion = DispatchGroup()
    completion.enter()
    DispatchQueue.global().async {
      BidirectionalFileDescriptorRelay.run(left: relayLeft, right: relayRight)
      completion.leave()
    }

    let outbound = Data("host-to-guest".utf8)
    try writeAll(outbound, to: left[1])
    #expect(try read(count: outbound.count, from: right[1]) == outbound)

    let inbound = Data("guest-to-host".utf8)
    try writeAll(inbound, to: right[1])
    #expect(try read(count: inbound.count, from: left[1]) == inbound)

    Darwin.shutdown(left[1], SHUT_RDWR)
    #expect(completion.wait(timeout: .now() + 2) == .success)
  }

  @MainActor
  @Test func validatesInstalledBundleDeviceGraphWhenRequested() throws {
    guard let bundlePath = ProcessInfo.processInfo.environment["CHEVALIER_VZ_TEST_BUNDLE_PATH"]
    else {
      return
    }
    let provisioningDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(
      at: provisioningDirectory,
      withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: provisioningDirectory) }
    let workspaceDirectory = FileManager.default.temporaryDirectory
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    try FileManager.default.createDirectory(
      at: workspaceDirectory,
      withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: workspaceDirectory) }

    let request = RunRequest(
      schemaVersion: 1,
      bundlePath: bundlePath,
      cpuCount: 4,
      memoryBytes: 8 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil,
      sharedDirectories: [
        SharedDirectoryRequest(
          hostPath: workspaceDirectory.path,
          name: "workspace",
          readOnly: false)
      ])
    let configuration = try MacOSRunConfiguration().make(request: request)

    #expect(configuration.networkDevices.isEmpty)
    #expect(configuration.storageDevices.count == 1)
    #expect(configuration.graphicsDevices.count == 1)
    #expect(configuration.keyboards.count == 1)
    #expect(configuration.pointingDevices.count == 1)
    #expect(configuration.entropyDevices.count == 1)
    #expect(configuration.socketDevices.count == 1)
    #expect(configuration.audioDevices.isEmpty)
    #expect(configuration.directorySharingDevices.count == 1)
    let fileSystems = configuration.directorySharingDevices.compactMap {
      $0 as? VZVirtioFileSystemDeviceConfiguration
    }
    #expect(
      fileSystems.map(\.tag) == [VZVirtioFileSystemDeviceConfiguration.macOSGuestAutomountTag])
    let share = fileSystems.first?.share as? VZMultipleDirectoryShare
    #expect(share?.directories["workspace"]?.isReadOnly == false)

    let natRequest = RunRequest(
      schemaVersion: 1,
      bundlePath: bundlePath,
      cpuCount: 4,
      memoryBytes: 8 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: provisioningDirectory.path,
      provisioningDirectoryReadOnly: true,
      networkMode: .natDevelopment,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: nil)
    let natConfiguration = try MacOSRunConfiguration().make(request: natRequest)
    #expect(natConfiguration.networkDevices.count == 1)
    let network = natConfiguration.networkDevices.first as? VZVirtioNetworkDeviceConfiguration
    #expect(network?.attachment is VZNATNetworkDeviceAttachment)
  }

  private func writeAll(_ data: Data, to descriptor: Int32) throws {
    try data.withUnsafeBytes { bytes in
      var offset = 0
      while offset < bytes.count {
        let count = Darwin.write(
          descriptor,
          bytes.baseAddress!.advanced(by: offset),
          bytes.count - offset)
        guard count > 0 else {
          throw RunError.loopbackRelay("test write failed")
        }
        offset += count
      }
    }
  }

  private func read(count: Int, from descriptor: Int32) throws -> Data {
    var data = Data(count: count)
    try data.withUnsafeMutableBytes { bytes in
      var offset = 0
      while offset < bytes.count {
        let result = Darwin.read(
          descriptor,
          bytes.baseAddress!.advanced(by: offset),
          bytes.count - offset)
        guard result > 0 else {
          throw RunError.loopbackRelay("test read failed")
        }
        offset += result
      }
    }
    return data
  }

  private func makeRunRequest(
    guestServiceRelays: [GuestServiceRelayRequest]
  ) -> RunRequest {
    RunRequest(
      schemaVersion: 1,
      bundlePath: "/tmp/template.bundle",
      cpuCount: 4,
      memoryBytes: 4 * 1024 * 1024 * 1024,
      provisioningDirectoryPath: nil,
      provisioningDirectoryReadOnly: nil,
      networkMode: nil,
      viewerMode: nil,
      loopbackRelayPort: nil,
      guestServiceRelays: guestServiceRelays)
  }
#endif
