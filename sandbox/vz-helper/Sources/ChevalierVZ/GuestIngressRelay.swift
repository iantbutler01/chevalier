import Darwin
import Foundation
@preconcurrency import Virtualization

final class GuestIngressRelay: @unchecked Sendable {
  static let guestVSockPort: UInt32 = 13_341
  static let maximumConcurrentSessions = 16

  let port: UInt16

  private final class Session: @unchecked Sendable {
    let tcpFileDescriptor: Int32
    let guestConnection: VZVirtioSocketConnection

    init(tcpFileDescriptor: Int32, guestConnection: VZVirtioSocketConnection) {
      self.tcpFileDescriptor = tcpFileDescriptor
      self.guestConnection = guestConnection
    }
  }

  private let socketDevice: VZVirtioSocketDevice
  private let listenerFileDescriptor: Int32
  private let acceptQueue = DispatchQueue(label: "dev.chevalier.vz.guest-ingress.accept")
  private let sessionLimit = ConcurrentSessionLimit(maximum: maximumConcurrentSessions)

  init(port: UInt16, socketDevice: VZVirtioSocketDevice) throws {
    guard port > 0 else {
      throw RunError.invalidRequest("guestIngressRelayPort must be greater than zero when present")
    }

    let descriptor = socket(AF_INET, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw Self.systemError("create TCP socket")
    }
    do {
      try Self.configureSocket(descriptor)
      var address = sockaddr_in()
      address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
      address.sin_family = sa_family_t(AF_INET)
      address.sin_port = port.bigEndian
      address.sin_addr = in_addr(s_addr: inet_addr(LoopbackTCPRelay.address))
      let result = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
          Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
      }
      guard result == 0 else {
        throw Self.systemError("bind \(LoopbackTCPRelay.address):\(port)")
      }
      guard Darwin.listen(descriptor, Int32(Self.maximumConcurrentSessions)) == 0 else {
        throw Self.systemError("listen on \(LoopbackTCPRelay.address):\(port)")
      }
    } catch {
      Darwin.close(descriptor)
      throw error
    }

    self.port = port
    self.socketDevice = socketDevice
    self.listenerFileDescriptor = descriptor
  }

  deinit {
    Darwin.close(listenerFileDescriptor)
  }

  func start() {
    NSLog(
      "chevalier-vz: guest ingress listening on %@:%u via guest vsock:%u",
      LoopbackTCPRelay.address,
      port,
      Self.guestVSockPort)
    acceptQueue.async { [self] in
      acceptConnections()
    }
  }

  private func acceptConnections() {
    while true {
      let descriptor = Darwin.accept(listenerFileDescriptor, nil, nil)
      if descriptor < 0 {
        if errno == EINTR { continue }
        NSLog("chevalier-vz: guest ingress accept failed: %@", Self.errnoDescription())
        return
      }
      do {
        try Self.configureSocket(descriptor)
      } catch {
        NSLog("chevalier-vz: guest ingress client setup failed: %@", error.localizedDescription)
        Darwin.close(descriptor)
        continue
      }
      guard sessionLimit.acquire() else {
        NSLog("chevalier-vz: rejected guest ingress client above cap")
        Darwin.close(descriptor)
        continue
      }

      Task { @MainActor [self] in
        do {
          let connection = try await socketDevice.connect(toPort: Self.guestVSockPort)
          startRelay(tcpFileDescriptor: descriptor, guestConnection: connection)
        } catch {
          Darwin.close(descriptor)
          sessionLimit.release()
          NSLog(
            "chevalier-vz: guest ingress vsock connect failed port=%u error=%@",
            Self.guestVSockPort,
            error.localizedDescription)
        }
      }
    }
  }

  private func startRelay(
    tcpFileDescriptor: Int32,
    guestConnection: VZVirtioSocketConnection
  ) {
    let session = Session(
      tcpFileDescriptor: tcpFileDescriptor,
      guestConnection: guestConnection)
    DispatchQueue.global(qos: .userInitiated).async { [self] in
      BidirectionalFileDescriptorRelay.run(
        left: session.tcpFileDescriptor,
        right: session.guestConnection.fileDescriptor)
      Darwin.close(session.tcpFileDescriptor)
      session.guestConnection.close()
      sessionLimit.release()
      NSLog("chevalier-vz: guest ingress session closed")
    }
  }

  private static func configureSocket(_ descriptor: Int32) throws {
    var enabled: Int32 = 1
    guard
      setsockopt(
        descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
        socklen_t(MemoryLayout<Int32>.size)) == 0
    else {
      throw systemError("configure socket")
    }
    _ = setsockopt(
      descriptor, SOL_SOCKET, SO_REUSEADDR, &enabled,
      socklen_t(MemoryLayout<Int32>.size))
  }

  private static func systemError(_ operation: String) -> RunError {
    RunError.loopbackRelay("\(operation): \(errnoDescription())")
  }

  private static func errnoDescription() -> String {
    String(cString: strerror(errno))
  }
}
