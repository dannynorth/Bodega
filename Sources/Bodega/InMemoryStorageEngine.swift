import Foundation

public actor InMemoryStorageEngine<Key: StorageKey, Value: Sendable>: StorageEngine {
    
    private struct InMemoryValue {
        let creationDate: Date
        var modificationDate: Date
        var value: Value
    }
    
    private var underlyingStorage: Dictionary<Key, InMemoryValue>
    
    public init() { 
        self.underlyingStorage = [:]
    }
    
    public func write(_ value: Value, key: Key) async throws {
        let now = Date()
        
        if underlyingStorage.keys.contains(key) {
            underlyingStorage[key]?.value = value
            underlyingStorage[key]?.modificationDate = now
        } else {
            underlyingStorage[key] = InMemoryValue(creationDate: now, modificationDate: now, value: value)
        }
    }
    
    public func write(_ keysAndValues: [(key: Key, value: Value)]) async throws {
        let now = Date()
        for (key, value) in keysAndValues {
            if underlyingStorage.keys.contains(key) {
                underlyingStorage[key]?.value = value
                underlyingStorage[key]?.modificationDate = now
            } else {
                underlyingStorage[key] = InMemoryValue(creationDate: now, modificationDate: now, value: value)
            }
        }
    }
    
    public func remove(keys: [Key]) async throws {
        for key in keys {
            underlyingStorage.removeValue(forKey: key)
        }
    }
    
    public func read(key: Key) async throws -> Value? {
        return underlyingStorage[key]?.value
    }
    
    public func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Value)] {
        return keys.compactMap { key in
            guard let value = underlyingStorage[key]?.value else { return nil }
            return (key, value)
        }
    }
    
    public func readAllValues() async throws -> [Value] {
        return underlyingStorage.values.map(\.value)
    }
    
    public func readAllKeysAndValues() async throws -> [(key: Key, value: Value)] {
        return underlyingStorage.map { ($0, $1.value) }
    }
    
    public func remove(key: Key) async throws {
        underlyingStorage.removeValue(forKey: key)
    }
    
    public func removeAllValues() async throws {
        underlyingStorage.removeAll()
    }
    
    public func keyCount() async throws -> Int {
        return underlyingStorage.count
    }
    
    public func keyExists(_ key: Key) async throws -> Bool {
        return underlyingStorage.keys.contains(key)
    }
    
    public func keysExist(_ keys: [Key]) async throws -> [Key] {
        return keys.filter { underlyingStorage.keys.contains($0) }
    }
    
    public func allKeys() async throws -> [Key] {
        return Array(underlyingStorage.keys)
    }
    
    public func createdAt(key: Key) async throws -> Date? {
        return underlyingStorage[key]?.creationDate
    }
    
    public func updatedAt(key: Key) async throws -> Date? {
        return underlyingStorage[key]?.modificationDate
    }
    
    
}
