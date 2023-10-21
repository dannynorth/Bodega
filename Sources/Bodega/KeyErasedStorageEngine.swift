import Foundation

/// A ``StorageEngine`` that's used for converting between one ``StorageKey`` type and another.
///
/// Use a ``KeyErasedStorageEngine`` when you want to compose two `StorageEngines`,
/// but don't want to require them to have the same ``StorageEngine.Key`` type.
/// With this, the outer engine can have a different ``StorageKey`` type from the inner engine, as long as
/// both key types are backed by the same "raw" storage, such as a `String`.
///
/// See ``ObjectStorageEngine``'s implementation for an example.
public actor KeyErasedStorageEngine<Key: StorageKey, Inner: StorageEngine>: StorageEngine where Key.KeyType == Inner.Key.KeyType {
    
    public typealias Value = Inner.Value
    
    private let engine: Inner
    
    public init(_ engine: Inner, keyType: Key.Type = Key.self) {
        self.engine = engine
    }
    
    public func write(_ value: Value, key: Key) async throws {
        try await engine.write(value, key: try Inner.Key(key))
    }
    
    public func write(_ keysAndValues: [(key: Key, value: Value)]) async throws {
        try await engine.write(keysAndValues.map {
            (try Inner.Key($0), $1)
        })
    }
    
    public func read(key: Key) async throws -> Value? {
        return try await engine.read(key: Inner.Key(key))
    }
    
    public func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Value)] {
        return try await engine.readKeysAndValues(keys: keys.map { try Inner.Key($0) })
            .map { (try Key($0), $1)}
    }
    
    public func readAllValues() async throws -> [Value] {
        return try await engine.readAllValues()
    }
    
    public func readAllKeysAndValues() async throws -> [(key: Key, value: Value)] {
        return try await engine.readAllKeysAndValues().map {
            (try Key($0), $1)
        }
    }
    
    public func remove(key: Key) async throws {
        try await engine.remove(key: Inner.Key(key))
    }
    
    public func remove(keys: [Key]) async throws {
        try await engine.remove(keys: keys.map { try Inner.Key($0) })
    }
    
    public func removeAllValues() async throws {
        try await engine.removeAllValues()
    }
    
    public func keyExists(_ key: Key) async throws -> Bool {
        return try await engine.keyExists(try Inner.Key(key))
    }
    
    public func keysExist(_ keys: [Key]) async throws -> [Key] {
        return try await engine.keysExist(keys.map { try Inner.Key($0) })
            .map { try Key($0) }
    }
    
    public func keyCount() async throws -> Int {
        return try await engine.keyCount()
    }
    
    public func allKeys() async throws -> [Key] {
        return try await engine.allKeys()
            .map { try Key($0) }
    }
    
    public func createdAt(key: Key) async throws -> Date? {
        return try await engine.createdAt(key: try Inner.Key(key))
    }
    
    public func updatedAt(key: Key) async throws -> Date? {
        return try await engine.updatedAt(key: try Inner.Key(key))
    }
    
}
