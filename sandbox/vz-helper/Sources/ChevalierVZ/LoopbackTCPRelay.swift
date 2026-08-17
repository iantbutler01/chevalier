import Darwin
import Foundation
import Virtualization

final class LoopbackTCPRelay: @unchecked Sendable {
  static let address = "127.0.0.1"

  private final class Session: @unchecked Sendable {
    let guestConnection: VZVirtioSocketConnection
    let tcpFileDescriptor: Int32

    init(guestConnection: VZVirtioSocketConnection, tcpFileDescriptor: Int32) {
      self.guestConnection = guestConnection
      self.tcpFileDescriptor = tcpFileDescriptor
    }
  }

  let port: UInt16

  private let listenerFileDescriptor: Int32
  private let acceptQueue = DispatchQueue(label: "dev.chevalier.vz.loopback-relay.accept")
  private let lock = NSLock()
  private var started = false
  private var pendingGuestConnection: VZVirtioSocketConnection?
  private var pendingTCPFileDescriptor: Int32?
  private var sessionActive = false

  init(port: UInt16) throws {
    guard port > 0 else {
      throw RunError.invalidRequest("loopbackRelayPort must be greater than zero when present")
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
      address.sin_addr = in_addr(s_addr: inet_addr(Self.address))
      let bindResult = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
          Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
        }
      }
      guard bindResult == 0 else {
        throw Self.systemError("bind \(Self.address):\(port)")
      }
      guard Darwin.listen(descriptor, 1) == 0 else {
        throw Self.systemError("listen on \(Self.address):\(port)")
      }
    } catch {
      Darwin.close(descriptor)
      throw error
    }

    self.port = port
    self.listenerFileDescriptor = descriptor
  }

  deinit {
    Darwin.close(listenerFileDescriptor)
    lock.withLock {
      if let pendingTCPFileDescriptor {
        Darwin.close(pendingTCPFileDescriptor)
      }
      pendingGuestConnection?.close()
    }
  }

  var guestConnectionCount: Int {
    lock.withLock { pendingGuestConnection == nil && !sessionActive ? 0 : 1 }
  }

  func start() {
    let shouldStart = lock.withLock {
      guard !started else { return false }
      started = true
      return true
    }
    guard shouldStart else { return }

    NSLog("chevalier-vz: loopback relay listening on %@:%u", Self.address, port)
    acceptQueue.async { [self] in
      acceptConnections()
    }
  }

  func attachGuestConnection(_ connection: VZVirtioSocketConnection) -> Bool {
    guard connection.fileDescriptor >= 0 else {
      NSLog("chevalier-vz: rejected closed guest control connection")
      return false
    }
    do {
      try Self.configureNoSigPipe(connection.fileDescriptor)
    } catch {
      NSLog("chevalier-vz: guest control socket setup failed: %@", error.localizedDescription)
      return false
    }

    var session: Session?
    let accepted = lock.withLock {
      guard pendingGuestConnection == nil, !sessionActive else { return false }
      pendingGuestConnection = connection
      session = makeSessionIfReady()
      return true
    }
    guard accepted else {
      NSLog("chevalier-vz: rejected additional guest control connection")
      return false
    }

    NSLog(
      "chevalier-vz: guest control connection ready for loopback relay source=%u fd=%d",
      connection.sourcePort,
      connection.fileDescriptor)
    if let session {
      startRelay(session)
    }
    return true
  }

  private func acceptConnections() {
    while true {
      let descriptor = Darwin.accept(listenerFileDescriptor, nil, nil)
      if descriptor < 0 {
        if errno == EINTR { continue }
        NSLog("chevalier-vz: loopback relay accept failed: %@", Self.errnoDescription())
        return
      }
      do {
        try Self.configureSocket(descriptor)
      } catch {
        NSLog("chevalier-vz: loopback client setup failed: %@", error.localizedDescription)
        Darwin.close(descriptor)
        continue
      }

      var session: Session?
      let accepted = lock.withLock {
        guard pendingTCPFileDescriptor == nil, !sessionActive else { return false }
        pendingTCPFileDescriptor = descriptor
        session = makeSessionIfReady()
        return true
      }
      guard accepted else {
        NSLog("chevalier-vz: rejected additional loopback relay client")
        Darwin.close(descriptor)
        continue
      }

      NSLog("chevalier-vz: accepted loopback relay client fd=%d", descriptor)
      if let session {
        startRelay(session)
      }
    }
  }

  private func makeSessionIfReady() -> Session? {
    guard let guestConnection = pendingGuestConnection,
      let tcpFileDescriptor = pendingTCPFileDescriptor
    else {
      return nil
    }
    pendingGuestConnection = nil
    pendingTCPFileDescriptor = nil
    sessionActive = true
    return Session(
      guestConnection: guestConnection,
      tcpFileDescriptor: tcpFileDescriptor)
  }

  private func startRelay(_ session: Session) {
    NSLog("chevalier-vz: loopback TCP and guest control streams paired")
    DispatchQueue.global(qos: .userInitiated).async { [self] in
      BidirectionalFileDescriptorRelay.run(
        left: session.tcpFileDescriptor,
        right: session.guestConnection.fileDescriptor)
      Darwin.close(session.tcpFileDescriptor)
      session.guestConnection.close()
      lock.withLock {
        sessionActive = false
      }
      NSLog("chevalier-vz: loopback relay session closed")
    }
  }

  private static func configureSocket(_ descriptor: Int32) throws {
    try configureNoSigPipe(descriptor)
    var enabled: Int32 = 1
    _ = setsockopt(
      descriptor, SOL_SOCKET, SO_REUSEADDR, &enabled,
      socklen_t(MemoryLayout<Int32>.size))
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
    RunError.loopbackRelay("\(operation): \(errnoDescription())")
  }

  private static func errnoDescription() -> String {
    String(cString: strerror(errno))
  }
}

enum BidirectionalFileDescriptorRelay {
  static func run(left: Int32, right: Int32) {
    let group = DispatchGroup()
    for (source, destination) in [(left, right), (right, left)] {
      group.enter()
      DispatchQueue.global(qos: .userInitiated).async {
        pump(source: source, destination: destination)
        Darwin.shutdown(left, SHUT_RDWR)
        Darwin.shutdown(right, SHUT_RDWR)
        group.leave()
      }
    }
    group.wait()
  }

  private static func pump(source: Int32, destination: Int32) {
    var buffer = [UInt8](repeating: 0, count: 64 * 1024)
    while true {
      let count = buffer.withUnsafeMutableBytes { bytes in
        Darwin.read(source, bytes.baseAddress!, bytes.count)
      }
      if count == 0 { return }
      if count < 0 {
        if errno == EINTR { continue }
        return
      }

      var written = 0
      while written < count {
        let result = buffer.withUnsafeBytes { bytes in
          Darwin.write(
            destination,
            bytes.baseAddress!.advanced(by: written),
            count - written)
        }
        if result < 0 {
          if errno == EINTR { continue }
          return
        }
        written += result
      }
    }
  }
}
