import Foundation

public protocol StorageKey: Hashable, Sendable {
    associatedtype KeyType: Hashable & Sendable
    
    var rawKey: KeyType { get }
    
    init(rawKey: KeyType)
}

extension StorageKey {
    
    public init<Other: StorageKey>(_ other: Other) where Other.KeyType == KeyType {
        self.init(rawKey: other.rawKey)
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
