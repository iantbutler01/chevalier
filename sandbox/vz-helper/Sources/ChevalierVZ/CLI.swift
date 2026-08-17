import Foundation

enum CLICommand: Equatable {
  case probe
  case inspectImage(ipsw: URL, expectedSHA256: String?)
  case install(request: URL, events: URL)
  case clone(request: URL)
  case run(request: URL)
  case version
  case help
}

enum CLIError: Error, Equatable {
  case missingCommand
  case missingValue(String)
  case invalidValue(option: String, value: String)
  case unexpectedArgument(String)
  case unknownCommand(String)
}

struct CLIParser {
  static let usage = """
    Usage:
      chevalier-vz probe
      chevalier-vz inspect-image --ipsw <path> [--expected-sha256 <digest>]
      chevalier-vz install --request <path> --events <path>
      chevalier-vz clone --request <path>
      chevalier-vz run --request <path>
      chevalier-vz version
      chevalier-vz help
    """ + "\n"

  static func parse(_ arguments: [String]) throws -> CLICommand {
    guard let command = arguments.first else {
      throw CLIError.missingCommand
    }

    switch command {
    case "probe":
      try requireNoArguments(Array(arguments.dropFirst()))
      return .probe
    case "inspect-image":
      return try parseInspectImage(Array(arguments.dropFirst()))
    case "install":
      return try parseInstall(Array(arguments.dropFirst()))
    case "clone":
      return try parseRequestPath(Array(arguments.dropFirst()), command: CLICommand.clone)
    case "run":
      return try parseRequestPath(Array(arguments.dropFirst()), command: CLICommand.run)
    case "version", "--version":
      try requireNoArguments(Array(arguments.dropFirst()))
      return .version
    case "help", "--help", "-h":
      try requireNoArguments(Array(arguments.dropFirst()))
      return .help
    default:
      throw CLIError.unknownCommand(command)
    }
  }

  private static func parseInstall(_ arguments: [String]) throws -> CLICommand {
    guard arguments.count >= 2 else {
      throw CLIError.missingValue("--request")
    }
    guard arguments[0] == "--request" else {
      throw CLIError.unexpectedArgument(arguments[0])
    }
    guard arguments.count >= 4 else {
      throw CLIError.missingValue("--events")
    }
    guard arguments[2] == "--events" else {
      throw CLIError.unexpectedArgument(arguments[2])
    }
    guard arguments.count == 4 else {
      throw CLIError.unexpectedArgument(arguments[4])
    }

    return .install(
      request: standardizedFileURL(arguments[1]),
      events: standardizedFileURL(arguments[3]))
  }

  private static func parseRequestPath(
    _ arguments: [String],
    command: (URL) -> CLICommand
  ) throws -> CLICommand {
    guard arguments.count >= 2 else {
      throw CLIError.missingValue("--request")
    }
    guard arguments[0] == "--request" else {
      throw CLIError.unexpectedArgument(arguments[0])
    }
    guard arguments.count == 2 else {
      throw CLIError.unexpectedArgument(arguments[2])
    }
    return command(standardizedFileURL(arguments[1]))
  }

  private static func parseInspectImage(_ arguments: [String]) throws -> CLICommand {
    guard let option = arguments.first else {
      throw CLIError.missingValue("--ipsw")
    }
    guard option == "--ipsw" else {
      throw CLIError.unexpectedArgument(option)
    }
    guard arguments.count >= 2 else {
      throw CLIError.missingValue("--ipsw")
    }
    guard arguments.count == 2 || arguments.count == 4 else {
      throw CLIError.unexpectedArgument(arguments[2])
    }

    let expectedSHA256: String?
    if arguments.count == 4 {
      guard arguments[2] == "--expected-sha256" else {
        throw CLIError.unexpectedArgument(arguments[2])
      }
      let value = arguments[3].lowercased()
      guard value.count == 64, value.allSatisfy(\.isHexDigit) else {
        throw CLIError.invalidValue(option: "--expected-sha256", value: arguments[3])
      }
      expectedSHA256 = value
    } else {
      expectedSHA256 = nil
    }
    return .inspectImage(
      ipsw: standardizedFileURL(arguments[1]),
      expectedSHA256: expectedSHA256)
  }

  private static func standardizedFileURL(_ path: String) -> URL {
    URL(fileURLWithPath: NSString(string: path).expandingTildeInPath).standardizedFileURL
  }

  private static func requireNoArguments(_ arguments: [String]) throws {
    if let argument = arguments.first {
      throw CLIError.unexpectedArgument(argument)
    }
  }
}
