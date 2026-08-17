import Foundation
import Testing

@testable import ChevalierVZ

@Test func validatesInstallRequestShape() throws {
  let request = makeInstallRequest()
  try request.validateShape()
  #expect(request.bundleURL.path == "/tmp/template.bundle")
  #expect(request.ipswURL.path == "/tmp/restore.ipsw")
}

@Test func rejectsSmallRootDisk() {
  let request = makeInstallRequest(rootDiskSizeBytes: 16 * 1024 * 1024 * 1024)
  #expect(throws: InstallError.self) {
    try request.validateShape()
  }
}

@Test func createsSparseRootDiskWithoutAllocatingLogicalSize() throws {
  let directory = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }

  let disk = directory.appendingPathComponent("Disk.img")
  let logicalSize: UInt64 = 32 * 1024 * 1024 * 1024
  try InstallBundleFileSystem().createSparseDisk(at: disk, size: logicalSize)
  let attributes = try FileManager.default.attributesOfItem(atPath: disk.path)

  #expect((attributes[.size] as? NSNumber)?.uint64Value == logicalSize)
  #expect((attributes[.systemFileNumber] as? NSNumber) != nil)
}

@Test func guardedBundleRefusesExistingTarget() throws {
  let directory = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }

  #expect(throws: InstallError.bundleAlreadyExists(directory.path)) {
    try InstallBundleFileSystem().prepareTarget(directory, requiredFreeSpaceBytes: 1)
  }
}

@Test func eventWriterEmitsMonotonicJSONLines() throws {
  let directory = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }

  let eventFile = directory.appendingPathComponent("install.events")
  let writer = try InstallEventWriter(url: eventFile)
  writer.emit(phase: "preflight", fraction: 0, force: true)
  writer.emit(phase: "installing", fraction: 0.5)
  writer.emit(phase: "installing", fraction: 0.4)
  writer.emit(phase: "complete", fraction: 1, force: true)
  try writer.throwIfFailed()

  let lines = try String(contentsOf: eventFile, encoding: .utf8).split(separator: "\n")
  let decoder = JSONDecoder()
  let events = try lines.map { try decoder.decode(InstallEvent.self, from: Data($0.utf8)) }
  #expect(events.map(\.sequence) == [1, 2, 3])
  #expect(events.map(\.fractionCompleted) == [0, 0.5, 1])
}

private func makeInstallRequest(
  rootDiskSizeBytes: UInt64 = 64 * 1024 * 1024 * 1024
) -> InstallRequest {
  InstallRequest(
    schemaVersion: 1,
    bundlePath: "/tmp/template.bundle",
    ipswPath: "/tmp/restore.ipsw",
    expectedSHA256: String(repeating: "a", count: 64),
    rootDiskSizeBytes: rootDiskSizeBytes,
    requiredFreeSpaceBytes: 30 * 1024 * 1024 * 1024,
    cpuCount: 4,
    memoryBytes: 8 * 1024 * 1024 * 1024)
}
