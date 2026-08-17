import Foundation
import Testing

@testable import ChevalierVZ

@Test func parsesProbe() throws {
  #expect(try CLIParser.parse(["probe"]) == .probe)
}

@Test func parsesLocalRestoreImage() throws {
  let command = try CLIParser.parse(["inspect-image", "--ipsw", "/tmp/restore.ipsw"])
  #expect(
    command
      == .inspectImage(
        ipsw: URL(fileURLWithPath: "/tmp/restore.ipsw"), expectedSHA256: nil))
}

@Test func parsesExpectedRestoreImageDigest() throws {
  let digest = String(repeating: "a", count: 64)
  let command = try CLIParser.parse([
    "inspect-image", "--ipsw", "/tmp/restore.ipsw", "--expected-sha256", digest,
  ])
  #expect(
    command
      == .inspectImage(
        ipsw: URL(fileURLWithPath: "/tmp/restore.ipsw"), expectedSHA256: digest))
}

@Test func parsesInstallRequestAndEventPaths() throws {
  let command = try CLIParser.parse([
    "install", "--request", "/tmp/install.json", "--events", "/tmp/install.events",
  ])
  #expect(
    command
      == .install(
        request: URL(fileURLWithPath: "/tmp/install.json"),
        events: URL(fileURLWithPath: "/tmp/install.events")))
}

@Test func parsesRunRequestPath() throws {
  #expect(
    try CLIParser.parse(["run", "--request", "/tmp/run.json"])
      == .run(request: URL(fileURLWithPath: "/tmp/run.json")))
}

@Test func parsesCloneRequestPath() throws {
  #expect(
    try CLIParser.parse(["clone", "--request", "/tmp/clone.json"])
      == .clone(request: URL(fileURLWithPath: "/tmp/clone.json")))
}

@Test func rejectsInvalidRestoreImageDigest() throws {
  #expect(throws: CLIError.invalidValue(option: "--expected-sha256", value: "nope")) {
    try CLIParser.parse([
      "inspect-image", "--ipsw", "/tmp/restore.ipsw", "--expected-sha256", "nope",
    ])
  }
}

@Test func rejectsUnexpectedArguments() throws {
  #expect(throws: CLIError.unexpectedArgument("extra")) {
    try CLIParser.parse(["probe", "extra"])
  }
}

@Test func formatsOperatingSystemVersion() {
  let version = OperatingSystemVersion(majorVersion: 26, minorVersion: 6, patchVersion: 1)
  #expect(RestoreImageService.versionString(version) == "26.6.1")
}

@Test func hashesRestoreImageWithoutLoadingItIntoMemory() throws {
  let directory = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }

  let file = directory.appendingPathComponent("restore.ipsw")
  try Data("chevalier-vz".utf8).write(to: file)
  let metadata = try RestoreImageService.fileMetadata(file)

  #expect(metadata.size == 12)
  #expect(metadata.sha256 == "f6676ecea23c1d25bda675e9b340585c560f60c3c9f40e212e693ed204f26a3d")
}

@Test func rejectsMismatchedDigestBeforeLoadingRestoreImage() async throws {
  let directory = FileManager.default.temporaryDirectory
    .appendingPathComponent(UUID().uuidString, isDirectory: true)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }

  let file = directory.appendingPathComponent("not-an-ipsw")
  try Data("chevalier-vz".utf8).write(to: file)
  let expected = String(repeating: "0", count: 64)

  do {
    _ = try await RestoreImageService().inspectImage(at: file, expectedSHA256: expected)
    Issue.record("expected checksum mismatch")
  } catch let error as HelperError {
    #expect(
      error
        == .checksumMismatch(
          expected: expected,
          actual: "f6676ecea23c1d25bda675e9b340585c560f60c3c9f40e212e693ed204f26a3d"))
  }
}
