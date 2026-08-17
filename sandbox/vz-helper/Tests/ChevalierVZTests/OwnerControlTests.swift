import Darwin
import Foundation
import Testing

@testable import ChevalierVZ

@Test func ownerControlOperationWireNamesAreStable() {
  #expect(
    OwnerControlOperation.allCases.map(\.rawValue) == [
      "status", "requestStop", "forceStop", "pause", "resume", "showViewer", "hideViewer",
      "shutdownHelper",
    ])
}

@Test func ownerControlFramesRoundTripAndEnforceBounds() throws {
  let request = OwnerControlRequest(
    protocolVersion: 1,
    id: "request-1",
    operation: .pause,
    expectedGeneration: "generation-7")
  let frame = try OwnerControlProtocol.encodeFrame(request)
  let decoded = try OwnerControlProtocol.decodeFrame(OwnerControlRequest.self, from: frame)
  #expect(decoded == request)

  let oversizedHeader = Data([0x00, 0x01, 0x00, 0x01])
  #expect(throws: OwnerControlError.invalidFrameLength(65_537)) {
    try OwnerControlProtocol.decodeFrame(OwnerControlRequest.self, from: oversizedHeader)
  }
  #expect(throws: OwnerControlError.truncatedFrame) {
    try OwnerControlProtocol.decodeFrame(
      OwnerControlRequest.self,
      from: frame.dropLast())
  }
}

@Test func ownerControlStatePolicyRestrictsMutations() {
  let running = OwnerRuntimeSnapshot(
    state: "running",
    canRequestStop: true,
    canForceStop: true,
    canPause: true,
    canResume: false)
  #expect(OwnerControlOperationPolicy.rejection(for: .status, snapshot: running) == nil)
  #expect(OwnerControlOperationPolicy.rejection(for: .pause, snapshot: running) == nil)
  #expect(OwnerControlOperationPolicy.rejection(for: .resume, snapshot: running) != nil)
  #expect(OwnerControlOperationPolicy.rejection(for: .showViewer, snapshot: running) == nil)
  #expect(OwnerControlOperationPolicy.rejection(for: .shutdownHelper, snapshot: running) != nil)

  let stopped = OwnerRuntimeSnapshot(
    state: "stopped",
    canRequestStop: false,
    canForceStop: false,
    canPause: false,
    canResume: false)
  #expect(OwnerControlOperationPolicy.rejection(for: .shutdownHelper, snapshot: stopped) == nil)
  #expect(OwnerControlOperationPolicy.rejection(for: .showViewer, snapshot: stopped) != nil)
}

@Test func runRequestRequiresGenerationForOwnerSocket() throws {
  let missingGeneration = ownerRunRequest(socketPath: "/tmp/owner.sock", generation: nil)
  #expect(
    throws: RunError.invalidRequest(
      "runtimeGeneration must be nonempty when ownerControlSocketPath is present")
  ) {
    try missingGeneration.validateShape()
  }

  let valid = ownerRunRequest(socketPath: "/tmp/owner.sock", generation: "generation-1")
  try valid.validateShape()
  #expect(valid.ownerControlSocketURL?.path == "/tmp/owner.sock")

  #expect(throws: RunError.invalidRequest("ownerControlSocketPath must be absolute")) {
    try ownerRunRequest(socketPath: "owner.sock", generation: "generation-1").validateShape()
  }
}

@Test func ownerControlServerCreatesPrivateParentAndSocket() throws {
  let root = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  let path = root.appendingPathComponent("control.sock").path
  do {
    let server = try OwnerControlServer(socketPath: path) { _, _ in }
    var parentMetadata = stat()
    var socketMetadata = stat()
    #expect(lstat(root.path, &parentMetadata) == 0)
    #expect(lstat(path, &socketMetadata) == 0)
    #expect(parentMetadata.st_mode & 0o777 == 0o700)
    #expect(socketMetadata.st_mode & 0o777 == 0o600)
    #expect(socketMetadata.st_uid == getuid())
    #expect(server.socketPath == path)
  }
  #expect(!FileManager.default.fileExists(atPath: path))
  try FileManager.default.removeItem(at: root)
}

@Test func ownerControlServerRefusesInsecureAndActivePaths() throws {
  let root = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
  defer { try? FileManager.default.removeItem(at: root) }
  try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: root.path)
  let path = root.appendingPathComponent("control.sock").path
  #expect(throws: OwnerControlError.insecureParent(root.path)) {
    try OwnerControlServer(socketPath: path) { _, _ in }
  }

  try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: root.path)
  do {
    let first = try OwnerControlServer(socketPath: path) { _, _ in }
    #expect(first.socketPath == path)
    #expect(throws: OwnerControlError.activeOwner(path)) {
      try OwnerControlServer(socketPath: path) { _, _ in }
    }
  }
}

@Test func ownerControlServerPreservesFilesAndReplacesOnlyStaleOwnedSockets() throws {
  let root = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(
    at: root,
    withIntermediateDirectories: false,
    attributes: [.posixPermissions: 0o700])
  defer { try? FileManager.default.removeItem(at: root) }

  let occupiedPath = root.appendingPathComponent("occupied.sock").path
  try Data("preserve".utf8).write(to: URL(fileURLWithPath: occupiedPath))
  #expect(throws: OwnerControlError.pathOccupied(occupiedPath)) {
    try OwnerControlServer(socketPath: occupiedPath) { _, _ in }
  }
  #expect(try Data(contentsOf: URL(fileURLWithPath: occupiedPath)) == Data("preserve".utf8))

  let stalePath = root.appendingPathComponent("stale.sock").path
  try createStaleSocket(at: stalePath)
  do {
    let server = try OwnerControlServer(socketPath: stalePath) { _, _ in }
    var metadata = stat()
    #expect(lstat(stalePath, &metadata) == 0)
    #expect(metadata.st_mode & S_IFMT == S_IFSOCK)
    #expect(server.socketPath == stalePath)
  }
}

private func ownerRunRequest(socketPath: String?, generation: String?) -> RunRequest {
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
    guestServiceRelays: nil,
    ownerControlSocketPath: socketPath,
    runtimeGeneration: generation)
}

private func createStaleSocket(at path: String) throws {
  let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
  guard descriptor >= 0 else {
    throw OwnerControlError.io("test socket failed")
  }
  defer { Darwin.close(descriptor) }
  let bytes = Array(path.utf8)
  var address = sockaddr_un()
  guard bytes.count < MemoryLayout.size(ofValue: address.sun_path) else {
    throw OwnerControlError.invalidSocketPath(path)
  }
  address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
  address.sun_family = sa_family_t(AF_UNIX)
  withUnsafeMutableBytes(of: &address.sun_path) { destination in
    destination.copyBytes(from: bytes)
    destination[bytes.count] = 0
  }
  let result = withUnsafePointer(to: &address) { pointer in
    pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
      Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
    }
  }
  guard result == 0 else {
    throw OwnerControlError.io("test bind failed")
  }
}
