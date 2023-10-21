import Foundation

public struct DiskStorageKey: StorageKey, ExpressibleByStringLiteral {
    
    public let rawKey: String
    
    internal let sanitizedValue: String
    
    public init(rawKey: String) {
        self.rawKey = rawKey
        self.sanitizedValue = Data(rawKey.utf8)
            .base64EncodedString()
            .replacingOccurrences(of: "=", with: "-")
    }
    
    public init(stringLiteral value: String) {
        self.init(rawKey: value)
    }
    
    internal init?(sanitized: String) {
        guard let data = Data(base64Encoded: sanitized.replacingOccurrences(of: "-", with: "=")) else {
            return nil
        }
        
        guard let plainText = String(data: data, encoding: .utf8) else {
            return nil
        }
        
        self.sanitizedValue = sanitized
        self.rawKey = plainText
    }
    
}
