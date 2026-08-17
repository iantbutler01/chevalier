import Foundation
import Testing

@testable import ChevalierVZ

@Test func cloneRequestUsesExactSchemaAndPaths() throws {
  let data = Data(
    """
    {
      "schemaVersion": 1,
      "sourceBundlePath": "/tmp/source.bundle",
      "destinationBundlePath": "/tmp/destination.bundle"
    }
    """.utf8)
  let request = try JSONDecoder().decode(CloneRequest.self, from: data)
  try request.validateShape()
  #expect(request.sourceBundleURL.path == "/tmp/source.bundle")
  #expect(request.destinationBundleURL.path == "/tmp/destination.bundle")
}

@Test func cloneRequestRejectsRelativeSameAndNestedDestinations() {
  #expect(throws: CloneError.invalidRequest("sourceBundlePath must be absolute")) {
    try CloneRequest(
      schemaVersion: 1,
      sourceBundlePath: "relative.bundle",
      destinationBundlePath: "/tmp/destination.bundle"
    ).validateShape()
  }
  #expect(throws: CloneError.invalidRequest("source and destination bundle paths must differ")) {
    try CloneRequest(
      schemaVersion: 1,
      sourceBundlePath: "/tmp/source.bundle",
      destinationBundlePath: "/tmp/source.bundle"
    ).validateShape()
  }
  #expect(throws: CloneError.invalidRequest("destination bundle must not be inside source bundle"))
  {
    try CloneRequest(
      schemaVersion: 1,
      sourceBundlePath: "/tmp/source.bundle",
      destinationBundlePath: "/tmp/source.bundle/clone.bundle"
    ).validateShape()
  }
}

@MainActor
@Test func clonesInstalledBundleWithFreshIdentityWhenRequested() throws {
  guard let sourcePath = ProcessInfo.processInfo.environment["CHEVALIER_VZ_TEST_BUNDLE_PATH"] else {
    return
  }
  let destination = URL(fileURLWithPath: sourcePath)
    .deletingLastPathComponent()
    .appendingPathComponent("clone-test-\(UUID().uuidString).bundle", isDirectory: true)
  defer { try? FileManager.default.removeItem(at: destination) }

  let sourcePaths = InstallBundlePaths(root: URL(fileURLWithPath: sourcePath))
  let sourceIdentifier = try Data(contentsOf: sourcePaths.machineIdentifier)
  let sourceAuxiliaryStorage = try RestoreImageService.fileMetadata(
    sourcePaths.auxiliaryStorage)
  let report = try MacOSCloneService().clone(
    request: CloneRequest(
      schemaVersion: 1,
      sourceBundlePath: sourcePath,
      destinationBundlePath: destination.path))
  let destinationPaths = InstallBundlePaths(root: destination)
  let destinationIdentifier = try Data(contentsOf: destinationPaths.machineIdentifier)
  let destinationAuxiliaryStorage = try RestoreImageService.fileMetadata(
    destinationPaths.auxiliaryStorage)
  let manifest = try JSONDecoder().decode(
    TemplateManifest.self,
    from: Data(contentsOf: destinationPaths.manifest))

  #expect(sourceIdentifier != destinationIdentifier)
  #expect(sourcePaths.auxiliaryStorage.path != destinationPaths.auxiliaryStorage.path)
  #expect(destinationAuxiliaryStorage.sha256 == sourceAuxiliaryStorage.sha256)
  #expect(report.auxiliaryStorageSHA256 == sourceAuxiliaryStorage.sha256)
  #expect(report.machineIdentifierSHA256 == manifest.templateMachineIdentifierSHA256)
  #expect(report.destinationBundlePath == destination.path)
  #expect(report.diskCloneMethod == "clonefile")
}
