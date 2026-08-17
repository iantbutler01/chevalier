import Foundation

enum JSONOutput {
  static func write<Value: Encodable>(
    _ value: Value,
    to fileHandle: FileHandle = .standardOutput
  ) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    var data = try encoder.encode(value)
    data.append(0x0A)
    try fileHandle.write(contentsOf: data)
  }
}
