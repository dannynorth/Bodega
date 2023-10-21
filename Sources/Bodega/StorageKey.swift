import Foundation

public protocol StorageKey: Hashable, Sendable {
    associatedtype KeyType: Hashable & Sendable
    
    var rawKey: KeyType { get }
    
    init(rawKey: KeyType) throws
}

extension StorageKey {
    
    public init<Other: StorageKey>(_ other: Other) throws where Other.KeyType == KeyType {
        try self.init(rawKey: other.rawKey)
    }
    
}

extension String: StorageKey {
    
    public var rawKey: String { self }
    
    public init(rawKey: String) {
        self = rawKey
    }
    
}

extension Int: StorageKey {
    
    public var rawKey: Int { self }
    
    public init(rawKey: Int) {
        self = rawKey
    }
    
}

extension UUID: StorageKey {
    
    public var rawKey: String { uuidString }
    
    public init(rawKey: String) throws {
        guard let uuid = UUID(uuidString: rawKey) else {
            throw CocoaError(.coderInvalidValue, userInfo: ["rawKey": rawKey])
        }
        self = uuid
    }
    
}
