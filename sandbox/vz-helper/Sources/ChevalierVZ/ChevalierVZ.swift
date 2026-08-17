import Darwin
import Foundation

@main
struct ChevalierVZ {
  static func main() async {
    do {
      let command = try CLIParser.parse(Array(CommandLine.arguments.dropFirst()))
      switch command {
      case .probe:
        try JSONOutput.write(try await RestoreImageService().probe())
      case .inspectImage(let url, let expectedSHA256):
        try JSONOutput.write(
          try await RestoreImageService().inspectImage(
            at: url,
            expectedSHA256: expectedSHA256))
      case .install(let requestURL, let eventsURL):
        let data = try Data(contentsOf: requestURL)
        let request = try JSONDecoder().decode(InstallRequest.self, from: data)
        try JSONOutput.write(
          try await MacOSInstallService().install(request: request, eventsURL: eventsURL))
      case .clone(let requestURL):
        let data = try Data(contentsOf: requestURL)
        let request = try JSONDecoder().decode(CloneRequest.self, from: data)
        try JSONOutput.write(try MacOSCloneService().clone(request: request))
      case .run(let requestURL):
        let data = try Data(contentsOf: requestURL)
        let request = try JSONDecoder().decode(RunRequest.self, from: data)
        try await MacOSVirtualMachineApplication.launch(request: request)
      case .version:
        try JSONOutput.write(VersionReport.current)
      case .help:
        FileHandle.standardOutput.write(Data(CLIParser.usage.utf8))
      }
    } catch {
      let report = ErrorReport(error: error)
      try? JSONOutput.write(report, to: .standardError)
      exit(report.exitCode)
    }
  }
}
