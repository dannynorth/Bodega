import Foundation

public struct DiskStorageKey: StorageKey, ExpressibleByStringLiteral {
    
    public let rawValue: String
    
    internal let sanitizedValue: String
    
    public init(_ value: String) {
        self.rawValue = value
        self.sanitizedValue = Data(value.utf8)
            .base64EncodedString()
            .replacingOccurrences(of: "=", with: "-")
    }
    
    public init(stringLiteral value: String) {
        self.init(value)
    }
    
    internal init?(sanitized: String) {
        guard let data = Data(base64Encoded: sanitized.replacingOccurrences(of: "-", with: "=")) else {
            return nil
        }
        
        guard let plainText = String(data: data, encoding: .utf8) else {
            return nil
        }
        
        self.sanitizedValue = sanitized
        self.rawValue = plainText
    }
    
}
