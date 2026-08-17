import Darwin
import Foundation
import Virtualization

final class ConcurrentSessionLimit: @unchecked Sendable {
  let maximum: Int

  private let lock = NSLock()
  private var active = 0

  init(maximum: Int) {
    precondition(maximum > 0)
    self.maximum = maximum
  }

  var activeCount: Int {
    lock.withLock { active }
  }

  func acquire() -> Bool {
    lock.withLock {
      guard active < maximum else { return false }
      active += 1
      return true
    }
  }

  func release() {
    lock.withLock {
      precondition(active > 0)
      active -= 1
    }
  }
}

final class GuestServiceRelay: NSObject, @unchecked Sendable, VZVirtioSocketListenerDelegate {
  static let maximumConcurrentSessions = 16

  private final class Session: @unchecked Sendable {
    let guestConnection: VZVirtioSocketConnection

    init(guestConnection: VZVirtioSocketConnection) {
      self.guestConnection = guestConnection
    }
  }

  let vsockPort: UInt32
  let hostLoopbackPort: UInt16

  private let sessionLimit = ConcurrentSessionLimit(maximum: maximumConcurrentSessions)

  init(vsockPort: UInt32, hostLoopbackPort: UInt16) {
    self.vsockPort = vsockPort
    self.hostLoopbackPort = hostLoopbackPort
    super.init()
  }

  var activeSessionCount: Int {
    sessionLimit.activeCount
  }

  func listener(
    _ listener: VZVirtioSocketListener,
    shouldAcceptNewConnection connection: VZVirtioSocketConnection,
    from socketDevice: VZVirtioSocketDevice
  ) -> Bool {
    guard connection.fileDescriptor >= 0 else {
      NSLog("chevalier-vz: rejected closed guest service connection vsock=%u", vsockPort)
      return false
    }
    guard sessionLimit.acquire() else {
      NSLog(
        "chevalier-vz: rejected guest service connection above cap vsock=%u cap=%d",
        vsockPort,
        Self.maximumConcurrentSessions)
      return false
    }
    do {
      try Self.configureNoSigPipe(connection.fileDescriptor)
    } catch {
      sessionLimit.release()
      NSLog(
        "chevalier-vz: guest service socket setup failed vsock=%u error=%@",
        vsockPort,
        error.localizedDescription)
      return false
    }

    let session = Session(guestConnection: connection)
    NSLog(
      "chevalier-vz: accepted guest service connection vsock=%u active=%d",
      vsockPort,
      activeSessionCount)
    DispatchQueue.global(qos: .userInitiated).async { [self] in
      run(session)
    }
    return true
  }

  private func run(_ session: Session) {
    let tcpFileDescriptor: Int32
    do {
      tcpFileDescriptor = try Self.connectToLoopback(port: hostLoopbackPort)
    } catch {
      session.guestConnection.close()
      sessionLimit.release()
      NSLog(
        "chevalier-vz: guest service loopback connect failed vsock=%u target=%@:%u error=%@",
        vsockPort,
        LoopbackTCPRelay.address,
        hostLoopbackPort,
        error.localizedDescription)
      return
    }

    NSLog(
      "chevalier-vz: guest service streams paired vsock=%u target=%@:%u",
      vsockPort,
      LoopbackTCPRelay.address,
      hostLoopbackPort)
    BidirectionalFileDescriptorRelay.run(
      left: session.guestConnection.fileDescriptor,
      right: tcpFileDescriptor)
    Darwin.close(tcpFileDescriptor)
    session.guestConnection.close()
    sessionLimit.release()
    NSLog(
      "chevalier-vz: guest service relay closed vsock=%u active=%d",
      vsockPort,
      activeSessionCount)
  }

  private static func connectToLoopback(port: UInt16) throws -> Int32 {
    let descriptor = socket(AF_INET, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw systemError("create TCP socket")
    }
    do {
      try configureNoSigPipe(descriptor)
      var address = sockaddr_in()
      address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
      address.sin_family = sa_family_t(AF_INET)
      address.sin_port = port.bigEndian
      address.sin_addr = in_addr(s_addr: inet_addr(LoopbackTCPRelay.address))
      let result = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
          Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
      }
      guard result == 0 else {
        throw systemError("connect \(LoopbackTCPRelay.address):\(port)")
      }
      return descriptor
    } catch {
      Darwin.close(descriptor)
      throw error
    }
  }

  private static func configureNoSigPipe(_ descriptor: Int32) throws {
    var enabled: Int32 = 1
    guard
      setsockopt(
        descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
        socklen_t(MemoryLayout<Int32>.size)) == 0
    else {
      throw systemError("configure socket")
    }
  }

  private static func systemError(_ operation: String) -> RunError {
    RunError.loopbackRelay("\(operation): \(String(cString: strerror(errno)))")
  }
}
