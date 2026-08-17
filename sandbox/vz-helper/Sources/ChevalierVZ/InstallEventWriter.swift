import Darwin
import Foundation

final class InstallEventWriter: @unchecked Sendable {
  private let lock = NSLock()
  private let handle: FileHandle
  private var sequence: UInt64 = 0
  private var lastFraction = -1.0
  private var outputError: Error?

  init(url: URL) throws {
    let descriptor = open(url.path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, S_IRUSR | S_IWUSR)
    guard descriptor >= 0 else {
      if errno == EEXIST {
        throw InstallError.eventFileAlreadyExists(url.path)
      }
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    handle = FileHandle(fileDescriptor: descriptor, closeOnDealloc: true)
  }

  func emit(phase: String, fraction: Double, message: String? = nil, force: Bool = false) {
    lock.lock()
    defer { lock.unlock() }
    guard outputError == nil else { return }

    let boundedFraction = min(max(fraction, 0), 1)
    guard force || boundedFraction >= lastFraction + 0.001 else { return }
    lastFraction = max(lastFraction, boundedFraction)
    sequence += 1
    let event = InstallEvent(
      schemaVersion: 1,
      sequence: sequence,
      type: "installProgress",
      timestamp: Date().ISO8601Format(),
      phase: phase,
      fractionCompleted: lastFraction,
      message: message)
    do {
      let encoder = JSONEncoder()
      encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
      var data = try encoder.encode(event)
      data.append(0x0a)
      try handle.write(contentsOf: data)
      try handle.synchronize()
    } catch {
      outputError = error
    }
  }

  func throwIfFailed() throws {
    lock.lock()
    defer { lock.unlock() }
    if let outputError {
      throw InstallError.progressOutputFailed(outputError.localizedDescription)
    }
  }
}
