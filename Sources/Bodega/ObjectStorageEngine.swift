import Foundation
import Combine // for TopLevel(Encoder|Decoder)

/// A unified layer over a ``StorageEngine`` primitives, allowing you to read, write, and save Swift objects.
///
/// ``ObjectStorage`` is a higher level abstraction than a ``StorageEngine``, allowing you
/// to interact with Swift objects, never thinking about the persistence layer that's saving
/// objects under the hood.
///
/// The ``SQLiteStorageEngine`` is a safe, fast, and easy database to based on SQLite,
/// but if you prefer to use your own persistence layer or want to save your objects
/// to another location, you can use the `storage` parameter like so
/// ```
/// SQLiteStorageEngine(directory: .defaultStorageDirectory(appendingPath: "Assets"))
/// ```
public actor ObjectStorageEngine<Key: StorageKey, Value: Codable & Sendable>: StorageEngine {
    private let storage: any StorageEngine<Key, Data>

    // A property for performance reasons, to avoid creating a new encoder on every write, N times for array-based methods.
    private let encode: (Value) throws -> Data

    // A property for performance reasons, to avoid creating a new decoder on every read, N times for array-based methods.
    private let decode: (Data) throws -> Value

    /// Initializes a new ``ObjectStorage`` object for persisting `Object`s.
    /// - Parameter storage: A ``StorageEngine`` to initialize an ``ObjectStorage`` instance with.
    public init<S: StorageEngine>(storage: S) where S.Key.KeyType == Key.KeyType, S.Value == Data {
        self.init(storage: storage, encoder: JSONEncoder(), decoder: JSONDecoder())
    }
    
    public init<S: StorageEngine, E: TopLevelEncoder, D: TopLevelDecoder>(storage: S, encoder: E, decoder: D) where S.Key.KeyType == Key.KeyType, S.Value == Data, E.Output == Data, D.Input == Data {
        self.storage = KeyErasedStorageEngine(storage, keyType: Key.self)
        self.encode = { try encoder.encode($0) }
        self.decode = { try decoder.decode(Value.self, from: $0) }
    }
    
    /// Writes a `Value` based on the associated `Key`.
    /// - Parameters:
    ///   - value: The object being stored.
    ///   - key: A ``Key`` for matching a `Value`.
    public func write(_ value: Value, key: Key) async throws {
        let data = try self.encode(value)
        try await storage.write(data, key: key)
    }
    
    public func write(_ keysAndValues: [(key: Key, value: Value)]) async throws {
        let keysAndDatas = try keysAndValues.map { key, value in
            let data = try self.encode(value)
            return (key, data)
        }
        try await storage.write(keysAndDatas)
    }
    
    /// Reads an `Object` based on the associated ``CacheKey``.
    /// - Parameters:
    ///   - key: A ``CacheKey`` for matching an `Object`.
    /// - Returns: The object stored if it exists, nil if there is no `Object` stored for the ``CacheKey``.
    
    /// Reads a `Value` based on the associated `Key`
    /// - Parameter key: A `Key` for matching a `Value`
    /// - Returns: The value if it exists, `nil` if there is no `Value` stored for the `Key`
    /// - Throws: May throw an error if an error occurred reading the key
    public func read(key: Key) async throws -> Value? {
        guard let data = try await storage.read(key: key) else {
            return nil
        }
        return try self.decode(data)
    }
    
    public func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Value)] {
        let keysAndDatas = try await storage.readKeysAndValues(keys: keys)
        return try keysAndDatas.map { key, data in
            let value = try self.decode(data)
            return (key, value)
        }
    }
    
    public func readAllValues() async throws -> [Value] {
        let datas = try await storage.readAllValues()
        return try datas.map { data in
            return try self.decode(data)
        }
    }
    
    public func readAllKeysAndValues() async throws -> [(key: Key, value: Value)] {
        let keysAndDatas = try await storage.readAllKeysAndValues()
        return try keysAndDatas.map { key, data in
            let value = try self.decode(data)
            return (key, value)
        }
    }
    
    public func remove(key: Key) async throws {
        try await storage.remove(key: key)
    }
    
    public func remove(keys: [Key]) async throws {
        try await storage.remove(keys: keys)
    }
    
    public func removeAllValues() async throws {
        try await storage.removeAllValues()
    }
    
    public func keyExists(_ key: Key) async throws -> Bool {
        return try await storage.keyExists(key)
    }
    
    public func keysExist(_ keys: [Key]) async throws -> [Key] {
        return try await storage.keysExist(keys)
    }
    
    public func keyCount() async throws -> Int {
        return try await storage.keyCount()
    }
    
    public func allKeys() async throws -> [Key] {
        return try await storage.allKeys()
    }
    
    public func createdAt(key: Key) async throws -> Date? {
        return try await storage.createdAt(key: key)
    }
    
    public func updatedAt(key: Key) async throws -> Date? {
        return try await storage.updatedAt(key: key)
    }
}
