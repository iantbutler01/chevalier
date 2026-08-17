import Darwin
import Foundation

final class OwnerControlServer: @unchecked Sendable {
  typealias Handler =
    @Sendable (
      OwnerControlRequest,
      @escaping @Sendable (OwnerControlResponse) -> Void
    ) -> Void

  static let maximumConcurrentConnections = 16

  let socketPath: String

  private let listenerFileDescriptor: Int32
  private let socketIdentity: (device: dev_t, inode: ino_t)
  private let handler: Handler
  private let acceptQueue = DispatchQueue(label: "dev.chevalier.vz.owner-control.accept")
  private let connectionQueue = DispatchQueue(
    label: "dev.chevalier.vz.owner-control.connection",
    attributes: .concurrent)
  private let connectionLimit = ConcurrentSessionLimit(maximum: maximumConcurrentConnections)
  private let lock = NSLock()
  private var started = false

  init(socketPath: String, handler: @escaping Handler) throws {
    self.socketPath = socketPath
    self.handler = handler
    try Self.prepareSocketPath(socketPath)

    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else {
      throw Self.systemError("create AF_UNIX socket")
    }
    var didBind = false
    var boundIdentity: (device: dev_t, inode: ino_t)?
    do {
      try Self.configureTimeouts(descriptor)
      var address = try Self.socketAddress(path: socketPath)
      let result = withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
          Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
      }
      guard result == 0 else {
        throw Self.systemError("bind \(socketPath)")
      }
      didBind = true
      var metadata = stat()
      guard lstat(socketPath, &metadata) == 0,
        metadata.st_mode & S_IFMT == S_IFSOCK,
        metadata.st_uid == getuid()
      else {
        throw OwnerControlError.pathOccupied(socketPath)
      }
      boundIdentity = (metadata.st_dev, metadata.st_ino)
      guard chmod(socketPath, 0o600) == 0 else {
        throw Self.systemError("chmod \(socketPath)")
      }
      guard Darwin.listen(descriptor, Int32(Self.maximumConcurrentConnections)) == 0 else {
        throw Self.systemError("listen \(socketPath)")
      }
      listenerFileDescriptor = descriptor
      socketIdentity = (metadata.st_dev, metadata.st_ino)
    } catch {
      Darwin.close(descriptor)
      if didBind, let boundIdentity {
        Self.unlinkSameSocketIfPresent(socketPath, identity: boundIdentity)
      }
      throw error
    }
  }

  deinit {
    Darwin.close(listenerFileDescriptor)
    unlinkOwnedSocketIfUnchanged()
  }

  func start() {
    let shouldStart = lock.withLock {
      guard !started else { return false }
      started = true
      return true
    }
    guard shouldStart else { return }
    acceptQueue.async { [self] in
      acceptConnections()
    }
  }

  private func acceptConnections() {
    while true {
      let descriptor = Darwin.accept(listenerFileDescriptor, nil, nil)
      if descriptor < 0 {
        if errno == EINTR { continue }
        return
      }
      guard connectionLimit.acquire() else {
        Darwin.close(descriptor)
        continue
      }
      connectionQueue.async { [self] in
        handleConnection(descriptor)
      }
    }
  }

  private func handleConnection(_ descriptor: Int32) {
    do {
      try Self.configureTimeouts(descriptor)
      var peerUID = uid_t()
      var peerGID = gid_t()
      guard getpeereid(descriptor, &peerUID, &peerGID) == 0 else {
        throw Self.systemError("getpeereid")
      }
      guard peerUID == getuid() else {
        throw OwnerControlError.peerUIDMismatch(expected: getuid(), actual: peerUID)
      }
      let request = try Self.readRequest(from: descriptor)
      handler(request) { [self] response in
        connectionQueue.async {
          try? Self.writeResponse(response, to: descriptor)
          Darwin.close(descriptor)
          self.connectionLimit.release()
        }
      }
    } catch {
      let response = OwnerControlResponse(
        protocolVersion: OwnerControlProtocol.version,
        id: "",
        ok: false,
        state: nil,
        generation: nil,
        pid: getpid(),
        error: error.localizedDescription)
      try? Self.writeResponse(response, to: descriptor)
      Darwin.close(descriptor)
      connectionLimit.release()
    }
  }

  private static func readRequest(from descriptor: Int32) throws -> OwnerControlRequest {
    let header = try readExactly(4, from: descriptor)
    let length = header.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    guard length > 0, length <= OwnerControlProtocol.maximumFrameBytes else {
      throw OwnerControlError.invalidFrameLength(Int(length))
    }
    let payload = try readExactly(Int(length), from: descriptor)
    return try JSONDecoder().decode(OwnerControlRequest.self, from: payload)
  }

  private static func writeResponse(
    _ response: OwnerControlResponse,
    to descriptor: Int32
  ) throws {
    try writeAll(OwnerControlProtocol.encodeFrame(response), to: descriptor)
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
          throw systemError("read")
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
          throw systemError("write")
        }
        offset += result
      }
    }
  }

  private static func prepareSocketPath(_ path: String) throws {
    guard path.hasPrefix("/") else { throw OwnerControlError.invalidSocketPath(path) }
    _ = try socketAddress(path: path)
    let parent = URL(fileURLWithPath: path).deletingLastPathComponent()
    let fileManager = FileManager.default
    if !fileManager.fileExists(atPath: parent.path) {
      try fileManager.createDirectory(
        at: parent,
        withIntermediateDirectories: true,
        attributes: [.posixPermissions: 0o700])
      try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: parent.path)
    }

    var parentMetadata = stat()
    guard lstat(parent.path, &parentMetadata) == 0,
      parentMetadata.st_mode & S_IFMT == S_IFDIR,
      parentMetadata.st_uid == getuid(),
      parentMetadata.st_mode & 0o077 == 0
    else {
      throw OwnerControlError.insecureParent(parent.path)
    }

    var metadata = stat()
    guard lstat(path, &metadata) == 0 else {
      if errno == ENOENT { return }
      throw systemError("lstat \(path)")
    }
    guard metadata.st_mode & S_IFMT == S_IFSOCK, metadata.st_uid == getuid() else {
      throw OwnerControlError.pathOccupied(path)
    }
    let staleIdentity = (device: metadata.st_dev, inode: metadata.st_ino)
    let connectionResult = stalePathConnectionResult(path: path)
    if connectionResult == 0 {
      throw OwnerControlError.activeOwner(path)
    }
    guard connectionResult == ECONNREFUSED else {
      throw OwnerControlError.pathOccupied(path)
    }
    unlinkSameSocketIfPresent(path, identity: staleIdentity)
    guard lstat(path, &metadata) != 0, errno == ENOENT else {
      throw OwnerControlError.pathOccupied(path)
    }
  }

  private static func stalePathConnectionResult(path: String) -> Int32 {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else { return errno }
    defer { Darwin.close(descriptor) }
    let existingFlags = fcntl(descriptor, F_GETFL)
    guard existingFlags >= 0, fcntl(descriptor, F_SETFL, existingFlags | O_NONBLOCK) == 0 else {
      return errno
    }
    guard var address = try? socketAddress(path: path) else { return EINVAL }
    let result = withUnsafePointer(to: &address) { pointer in
      pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
      }
    }
    return result == 0 ? 0 : errno
  }

  private static func socketAddress(path: String) throws -> sockaddr_un {
    let bytes = Array(path.utf8)
    var address = sockaddr_un()
    let capacity = MemoryLayout.size(ofValue: address.sun_path)
    guard !bytes.isEmpty, !bytes.contains(0), bytes.count < capacity else {
      throw OwnerControlError.invalidSocketPath(path)
    }
    address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
    address.sun_family = sa_family_t(AF_UNIX)
    withUnsafeMutableBytes(of: &address.sun_path) { destination in
      destination.copyBytes(from: bytes)
      destination[bytes.count] = 0
    }
    return address
  }

  private static func configureTimeouts(_ descriptor: Int32) throws {
    var timeout = timeval(tv_sec: 5, tv_usec: 0)
    for option in [SO_RCVTIMEO, SO_SNDTIMEO] {
      guard
        setsockopt(
          descriptor, SOL_SOCKET, option, &timeout,
          socklen_t(MemoryLayout<timeval>.size)) == 0
      else {
        throw systemError("configure socket timeout")
      }
    }
  }

  private func unlinkOwnedSocketIfUnchanged() {
    var metadata = stat()
    guard lstat(socketPath, &metadata) == 0,
      metadata.st_mode & S_IFMT == S_IFSOCK,
      metadata.st_uid == getuid(),
      metadata.st_dev == socketIdentity.device,
      metadata.st_ino == socketIdentity.inode
    else {
      return
    }
    unlink(socketPath)
  }

  private static func unlinkSameSocketIfPresent(
    _ path: String,
    identity: (device: dev_t, inode: ino_t)
  ) {
    var metadata = stat()
    guard lstat(path, &metadata) == 0,
      metadata.st_mode & S_IFMT == S_IFSOCK,
      metadata.st_uid == getuid(),
      metadata.st_dev == identity.device,
      metadata.st_ino == identity.inode
    else {
      return
    }
    unlink(path)
  }

  private static func systemError(_ operation: String) -> OwnerControlError {
    OwnerControlError.io("\(operation): \(String(cString: strerror(errno)))")
  }
}
