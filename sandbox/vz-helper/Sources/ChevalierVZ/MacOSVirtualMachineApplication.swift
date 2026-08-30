import AppKit
import Darwin
import Foundation
import Virtualization

final class GuestControlSocketListener: NSObject, VZVirtioSocketListenerDelegate {
  static let port = RunRequest.controlVSockPort

  private let lock = NSLock()
  private let relay: LoopbackTCPRelay?
  private var acceptedConnection: VZVirtioSocketConnection?

  init(relay: LoopbackTCPRelay? = nil) {
    self.relay = relay
  }

  var connectionCount: Int {
    relay?.guestConnectionCount ?? lock.withLock { acceptedConnection == nil ? 0 : 1 }
  }

  func listener(
    _ listener: VZVirtioSocketListener,
    shouldAcceptNewConnection connection: VZVirtioSocketConnection,
    from socketDevice: VZVirtioSocketDevice
  ) -> Bool {
    if let relay {
      return relay.attachGuestConnection(connection)
    }

    let accepted = lock.withLock {
      guard acceptedConnection == nil else { return false }
      acceptedConnection = connection
      return true
    }
    guard accepted else {
      NSLog("chevalier-vz: rejected additional guest control connection")
      return false
    }
    NSLog(
      "chevalier-vz: accepted guest control connection source=%u destination=%u fd=%d",
      connection.sourcePort,
      connection.destinationPort,
      connection.fileDescriptor)
    return true
  }
}

private enum GuestRuntimeConfigurationTransport {
  static func exchange(
    _ request: Data,
    _ descriptor: Int32
  ) throws -> GuestRuntimeConfigurationResponse {
    try configureTimeouts(descriptor)
    try writeAll(request, to: descriptor)
    let header = try readExactly(4, from: descriptor)
    let length = header.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    guard length > 0, length <= OwnerControlProtocol.maximumFrameBytes else {
      throw OwnerControlError.invalidFrameLength(Int(length))
    }
    let payload = try readExactly(Int(length), from: descriptor)
    return try JSONDecoder().decode(GuestRuntimeConfigurationResponse.self, from: payload)
  }

  private static func configureTimeouts(_ descriptor: Int32) throws {
    var timeout = timeval(tv_sec: 10, tv_usec: 0)
    for option in [SO_RCVTIMEO, SO_SNDTIMEO] {
      guard
        setsockopt(
          descriptor,
          SOL_SOCKET,
          option,
          &timeout,
          socklen_t(MemoryLayout<timeval>.size)) == 0
      else {
        throw OwnerControlError.io(
          "configure guest runtime socket: \(String(cString: strerror(errno)))")
      }
    }
  }

  private static func readExactly(_ count: Int, from descriptor: Int32) throws -> Data {
    var data = Data(count: count)
    try data.withUnsafeMutableBytes { bytes in
      var offset = 0
      while offset < count {
        let result = Darwin.read(
          descriptor,
          bytes.baseAddress!.advanced(by: offset),
          count - offset)
        if result == 0 { throw OwnerControlError.truncatedFrame }
        if result < 0 {
          if errno == EINTR { continue }
          if errno == EAGAIN || errno == EWOULDBLOCK {
            try waitForReadiness(descriptor, events: Int16(POLLIN))
            continue
          }
          throw OwnerControlError.io(
            "read guest runtime response: \(String(cString: strerror(errno)))")
        }
        offset += result
      }
    }
    return data
  }

  private static func writeAll(_ data: Data, to descriptor: Int32) throws {
    try data.withUnsafeBytes { bytes in
      var offset = 0
      while offset < bytes.count {
        let result = Darwin.write(
          descriptor,
          bytes.baseAddress!.advanced(by: offset),
          bytes.count - offset)
        if result < 0 {
          if errno == EINTR { continue }
          if errno == EAGAIN || errno == EWOULDBLOCK {
            try waitForReadiness(descriptor, events: Int16(POLLOUT))
            continue
          }
          throw OwnerControlError.io(
            "write guest runtime request: \(String(cString: strerror(errno)))")
        }
        offset += result
      }
    }
  }

  private static func waitForReadiness(_ descriptor: Int32, events: Int16) throws {
    var pollDescriptor = pollfd(fd: descriptor, events: events, revents: 0)
    while true {
      let result = Darwin.poll(&pollDescriptor, 1, 10_000)
      if result > 0 { return }
      if result == 0 {
        throw OwnerControlError.io("guest runtime socket timed out")
      }
      if errno != EINTR {
        throw OwnerControlError.io(
          "wait for guest runtime socket: \(String(cString: strerror(errno)))")
      }
    }
  }
}

@MainActor
final class MacOSVirtualMachineApplication: NSObject, NSWindowDelegate,
  @preconcurrency VZVirtualMachineDelegate
{
  private let application: NSApplication
  private let virtualMachine: VZVirtualMachine
  private let initialViewerMode: RunViewerMode
  private let socketListener: VZVirtioSocketListener
  private let socketDelegate: GuestControlSocketListener
  private let loopbackRelay: LoopbackTCPRelay?
  private let guestIngressRelay: GuestIngressRelay?
  private let guestServiceSocketListeners: [VZVirtioSocketListener]
  private let guestServiceRelays: [GuestServiceRelay]
  private let runtimeGeneration: String?
  private var ownerControlServer: OwnerControlServer?
  private var machineView: VZVirtualMachineView?
  private var viewerWindow: NSWindow?
  private var startFailure: Error?

  static func launch(request: RunRequest) async throws {
    let application = try MacOSVirtualMachineApplication(request: request)
    try await application.run()
  }

  init(
    request: RunRequest,
    application: NSApplication = .shared,
    configurationBuilder: MacOSRunConfiguration = MacOSRunConfiguration()
  ) throws {
    let configuration = try configurationBuilder.make(request: request)
    let virtualMachine = VZVirtualMachine(configuration: configuration)

    guard let socketDevice = virtualMachine.socketDevices.first as? VZVirtioSocketDevice else {
      throw RunError.missingVirtioSocketDevice
    }
    let loopbackRelay = try request.loopbackRelayPort.map(LoopbackTCPRelay.init(port:))
    let guestIngressRelay = try request.guestIngressRelayPort.map {
      try GuestIngressRelay(port: $0, socketDevice: socketDevice)
    }
    let socketListener = VZVirtioSocketListener()
    let socketDelegate = GuestControlSocketListener(relay: loopbackRelay)
    socketListener.delegate = socketDelegate
    socketDevice.setSocketListener(socketListener, forPort: GuestControlSocketListener.port)

    var guestServiceSocketListeners: [VZVirtioSocketListener] = []
    var guestServiceRelays: [GuestServiceRelay] = []
    for mapping in request.guestServiceRelays ?? [] {
      let relay = GuestServiceRelay(
        vsockPort: mapping.vsockPort,
        hostLoopbackPort: mapping.hostLoopbackPort)
      let listener = VZVirtioSocketListener()
      listener.delegate = relay
      socketDevice.setSocketListener(listener, forPort: mapping.vsockPort)
      guestServiceRelays.append(relay)
      guestServiceSocketListeners.append(listener)
    }

    self.application = application
    self.virtualMachine = virtualMachine
    self.initialViewerMode = request.effectiveViewerMode
    self.socketListener = socketListener
    self.socketDelegate = socketDelegate
    self.loopbackRelay = loopbackRelay
    self.guestIngressRelay = guestIngressRelay
    self.guestServiceSocketListeners = guestServiceSocketListeners
    self.guestServiceRelays = guestServiceRelays
    self.runtimeGeneration = request.runtimeGeneration
    super.init()
    virtualMachine.delegate = self
    if let socketPath = request.ownerControlSocketURL?.path {
      ownerControlServer = try OwnerControlServer(socketPath: socketPath) {
        [weak self] request, completion in
        Task { @MainActor [weak self] in
          guard let self else {
            completion(
              OwnerControlResponse(
                protocolVersion: OwnerControlProtocol.version,
                id: request.id,
                ok: false,
                state: nil,
                generation: nil,
                pid: getpid(),
                error: "VM owner is unavailable"))
            return
          }
          completion(await handleOwnerControlRequest(request))
        }
      }
    }
  }

  func run() async throws {
    application.setActivationPolicy(initialViewerMode == .window ? .regular : .accessory)
    if initialViewerMode == .window {
      showViewer()
      application.activate(ignoringOtherApps: true)
    }
    NSLog("chevalier-vz: viewer mode=%@", initialViewerMode.rawValue)
    NSLog(
      "chevalier-vz: listening for guest control connections on Virtio socket port %u",
      GuestControlSocketListener.port)
    for relay in guestServiceRelays {
      NSLog(
        "chevalier-vz: listening for guest service connections vsock=%u target=%@:%u cap=%d",
        relay.vsockPort,
        LoopbackTCPRelay.address,
        relay.hostLoopbackPort,
        GuestServiceRelay.maximumConcurrentSessions)
    }
    ownerControlServer?.start()
    guestIngressRelay?.start()
    if let ownerControlServer {
      NSLog(
        "chevalier-vz: owner control listening path=%@ generation=%@",
        ownerControlServer.socketPath,
        runtimeGeneration ?? "")
    }
    loopbackRelay?.start()
    NSLog("chevalier-vz: entering AppKit run loop before VM start")
    DispatchQueue.main.async { [weak self] in
      self?.startVirtualMachine()
    }
    application.run()
    if let startFailure {
      throw startFailure
    }
  }

  func showViewer() {
    if let viewerWindow {
      viewerWindow.makeKeyAndOrderFront(nil)
      application.activate()
      return
    }

    let machineView = VZVirtualMachineView(
      frame: NSRect(x: 0, y: 0, width: 1280, height: 720))
    machineView.virtualMachine = virtualMachine
    machineView.capturesSystemKeys = true
    machineView.automaticallyReconfiguresDisplay = true

    let viewerWindow = NSWindow(
      contentRect: machineView.frame,
      styleMask: [.titled, .closable, .miniaturizable, .resizable],
      backing: .buffered,
      defer: false)
    viewerWindow.title = "Chevalier macOS Guest"
    viewerWindow.contentView = machineView
    viewerWindow.delegate = self
    viewerWindow.center()

    self.machineView = machineView
    self.viewerWindow = viewerWindow
    viewerWindow.makeKeyAndOrderFront(nil)
    application.activate()
  }

  func hideViewer() {
    machineView?.virtualMachine = nil
    machineView = nil
    viewerWindow?.delegate = nil
    viewerWindow?.close()
    viewerWindow = nil
  }

  private func startVirtualMachine() {
    NSLog(
      "chevalier-vz: requesting VM start state=%@",
      Self.stateDescription(virtualMachine.state))
    virtualMachine.start { [weak self] result in
      Task { @MainActor [weak self] in
        guard let self else { return }
        switch result {
        case .success:
          NSLog(
            "chevalier-vz: VM start succeeded state=%@",
            Self.stateDescription(virtualMachine.state))
        case .failure(let error):
          startFailure = error
          NSLog(
            "chevalier-vz: VM start failed state=%@ error=%@",
            Self.stateDescription(virtualMachine.state),
            error.localizedDescription)
          application.terminate(nil)
        }
      }
    }
  }

  func windowWillClose(_ notification: Notification) {
    guard let window = notification.object as? NSWindow, window === viewerWindow else { return }
    machineView?.virtualMachine = nil
    machineView = nil
    viewerWindow = nil
    NSLog(
      "chevalier-vz: viewer closed; VM remains state=%@",
      Self.stateDescription(virtualMachine.state))
  }

  func guestDidStop(_ virtualMachine: VZVirtualMachine) {
    NSLog(
      "chevalier-vz: guest stopped state=%@",
      Self.stateDescription(virtualMachine.state))
    if ownerControlServer == nil {
      application.terminate(nil)
    }
  }

  func virtualMachine(_ virtualMachine: VZVirtualMachine, didStopWithError error: any Error) {
    NSLog(
      "chevalier-vz: guest stopped with error state=%@ error=%@",
      Self.stateDescription(virtualMachine.state),
      error.localizedDescription)
    if ownerControlServer == nil {
      application.terminate(nil)
    }
  }

  private func handleOwnerControlRequest(
    _ request: OwnerControlRequest
  ) async -> OwnerControlResponse {
    guard request.protocolVersion == OwnerControlProtocol.version else {
      return ownerResponse(
        request: request,
        ok: false,
        error: "unsupported protocolVersion \(request.protocolVersion)")
    }
    guard !request.id.isEmpty, request.id.utf8.count <= OwnerControlProtocol.maximumIDBytes else {
      return ownerResponse(
        request: request, ok: false, error: "id must contain 1...256 UTF-8 bytes")
    }
    if let expectedGeneration = request.expectedGeneration,
      expectedGeneration != runtimeGeneration
    {
      return ownerResponse(request: request, ok: false, error: "runtime generation mismatch")
    }

    let snapshot = ownerRuntimeSnapshot()
    if let rejection = OwnerControlOperationPolicy.rejection(
      for: request.operation,
      snapshot: snapshot)
    {
      return ownerResponse(request: request, ok: false, error: rejection)
    }

    do {
      switch request.operation {
      case .status:
        break
      case .requestStop:
        try virtualMachine.requestStop()
      case .forceStop:
        try await forceStopVirtualMachine()
      case .pause:
        try await pauseVirtualMachine()
      case .resume:
        try await resumeVirtualMachine()
      case .showViewer:
        showViewer()
      case .hideViewer:
        hideViewer()
      case .configureGuest:
        guard let configuration = request.guestConfiguration else {
          throw RunError.invalidRequest("configureGuest requires guestConfiguration")
        }
        try await configureGuest(configuration)
      case .shutdownHelper:
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) { [weak application] in
          application?.terminate(nil)
        }
      }
      return ownerResponse(request: request, ok: true, error: nil)
    } catch {
      return ownerResponse(request: request, ok: false, error: error.localizedDescription)
    }
  }

  private func ownerResponse(
    request: OwnerControlRequest,
    ok: Bool,
    error: String?
  ) -> OwnerControlResponse {
    OwnerControlResponse(
      protocolVersion: OwnerControlProtocol.version,
      id: request.id,
      ok: ok,
      state: Self.stateDescription(virtualMachine.state),
      generation: runtimeGeneration,
      pid: getpid(),
      error: error)
  }

  private func ownerRuntimeSnapshot() -> OwnerRuntimeSnapshot {
    OwnerRuntimeSnapshot(
      state: Self.stateDescription(virtualMachine.state),
      canRequestStop: virtualMachine.canRequestStop,
      canForceStop: virtualMachine.canStop,
      canPause: virtualMachine.canPause,
      canResume: virtualMachine.canResume)
  }

  private func forceStopVirtualMachine() async throws {
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
      virtualMachine.stop { error in
        if let error {
          continuation.resume(throwing: error)
        } else {
          continuation.resume()
        }
      }
    }
  }

  private func pauseVirtualMachine() async throws {
    try await withCheckedThrowingContinuation { continuation in
      virtualMachine.pause { continuation.resume(with: $0) }
    }
  }

  private func resumeVirtualMachine() async throws {
    try await withCheckedThrowingContinuation { continuation in
      virtualMachine.resume { continuation.resume(with: $0) }
    }
  }

  private func configureGuest(_ configuration: GuestRuntimeConfiguration) async throws {
    guard let socketDevice = virtualMachine.socketDevices.first as? VZVirtioSocketDevice else {
      throw RunError.missingVirtioSocketDevice
    }
    let connection = try await socketDevice.connect(toPort: 13_340)
    defer { connection.close() }
    let frame = try OwnerControlProtocol.encodeFrame(configuration)
    let response = try await withCheckedThrowingContinuation {
      (continuation: CheckedContinuation<GuestRuntimeConfigurationResponse, Error>) in
      let descriptor = connection.fileDescriptor
      DispatchQueue.global(qos: .userInitiated).async {
        continuation.resume(
          with: Result { try GuestRuntimeConfigurationTransport.exchange(frame, descriptor) })
      }
    }
    guard response.schemaVersion == 1 else {
      throw RunError.invalidRequest(
        "guest runtime configuration protocol mismatch: \(response.schemaVersion)")
    }
    guard response.ok else {
      throw RunError.invalidRequest(
        response.error ?? "guest runtime configuration was rejected")
    }
  }

  private static func stateDescription(_ state: VZVirtualMachine.State) -> String {
    switch state {
    case .stopped: "stopped"
    case .running: "running"
    case .paused: "paused"
    case .error: "error"
    case .starting: "starting"
    case .pausing: "pausing"
    case .resuming: "resuming"
    case .stopping: "stopping"
    case .saving: "saving"
    case .restoring: "restoring"
    @unknown default: "unknown(\(state.rawValue))"
    }
  }
}
