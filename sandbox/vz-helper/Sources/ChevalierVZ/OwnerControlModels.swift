import Foundation

enum OwnerControlOperation: String, Codable, CaseIterable {
  case status
  case requestStop
  case forceStop
  case pause
  case resume
  case showViewer
  case hideViewer
  case shutdownHelper
}

struct OwnerControlRequest: Codable, Equatable {
  let protocolVersion: Int
  let id: String
  let operation: OwnerControlOperation
  let expectedGeneration: String?
}

struct OwnerControlResponse: Codable, Equatable {
  let protocolVersion: Int
  let id: String
  let ok: Bool
  let state: String?
  let generation: String?
  let pid: Int32?
  let error: String?
}

enum OwnerControlProtocol {
  static let version = 1
  static let maximumFrameBytes = 64 * 1024
  static let maximumIDBytes = 256

  static func encodeFrame<T: Encodable>(_ value: T) throws -> Data {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    let payload = try encoder.encode(value)
    guard !payload.isEmpty, payload.count <= maximumFrameBytes else {
      throw OwnerControlError.invalidFrameLength(payload.count)
    }
    var length = UInt32(payload.count).bigEndian
    var frame = Data(bytes: &length, count: MemoryLayout<UInt32>.size)
    frame.append(payload)
    return frame
  }

  static func decodeFrame<T: Decodable>(_ type: T.Type, from frame: Data) throws -> T {
    guard frame.count >= MemoryLayout<UInt32>.size else {
      throw OwnerControlError.truncatedFrame
    }
    let length = frame.prefix(4).reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
    guard length > 0, length <= maximumFrameBytes else {
      throw OwnerControlError.invalidFrameLength(Int(length))
    }
    guard frame.count == 4 + Int(length) else {
      throw OwnerControlError.truncatedFrame
    }
    return try JSONDecoder().decode(type, from: frame.dropFirst(4))
  }
}

enum OwnerControlError: Error, Equatable, LocalizedError {
  case invalidFrameLength(Int)
  case truncatedFrame
  case io(String)
  case invalidSocketPath(String)
  case insecureParent(String)
  case pathOccupied(String)
  case activeOwner(String)
  case peerUIDMismatch(expected: uid_t, actual: uid_t)

  var errorDescription: String? {
    switch self {
    case .invalidFrameLength(let length):
      "invalid owner control frame length: \(length)"
    case .truncatedFrame:
      "truncated owner control frame"
    case .io(let message):
      "owner control I/O failed: \(message)"
    case .invalidSocketPath(let path):
      "invalid owner control socket path: \(path)"
    case .insecureParent(let path):
      "owner control socket parent must be a same-user 0700 directory: \(path)"
    case .pathOccupied(let path):
      "owner control socket path is occupied by a non-socket or foreign owner: \(path)"
    case .activeOwner(let path):
      "another owner is already listening at: \(path)"
    case .peerUIDMismatch(let expected, let actual):
      "owner control peer UID mismatch: expected \(expected), got \(actual)"
    }
  }
}

struct OwnerRuntimeSnapshot: Equatable {
  let state: String
  let canRequestStop: Bool
  let canForceStop: Bool
  let canPause: Bool
  let canResume: Bool
}

enum OwnerControlOperationPolicy {
  static func rejection(
    for operation: OwnerControlOperation,
    snapshot: OwnerRuntimeSnapshot
  ) -> String? {
    switch operation {
    case .status, .hideViewer:
      nil
    case .requestStop:
      snapshot.canRequestStop
        ? nil : "VM cannot request a graceful stop from state \(snapshot.state)"
    case .forceStop:
      snapshot.canForceStop ? nil : "VM cannot force stop from state \(snapshot.state)"
    case .pause:
      snapshot.canPause ? nil : "VM cannot pause from state \(snapshot.state)"
    case .resume:
      snapshot.canResume ? nil : "VM cannot resume from state \(snapshot.state)"
    case .showViewer:
      ["starting", "running", "paused"].contains(snapshot.state)
        ? nil : "viewer is unavailable in state \(snapshot.state)"
    case .shutdownHelper:
      ["stopped", "error"].contains(snapshot.state)
        ? nil : "helper shutdown requires stopped or error state, got \(snapshot.state)"
    }
  }
}
