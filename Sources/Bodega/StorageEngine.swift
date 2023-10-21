import Foundation

/// A ``StorageEngine`` represents a data storage mechanism for saving and persisting data.
///
/// A ``StorageEngine`` is a construct you can build that plugs into ``ObjectStorage``
/// to use for persisting data.
///
/// This library has two implementations of ``StorageEngine``, ``DiskStorageEngine`` and ``SQLiteStorageEngine``.
/// Both of these can serve as inspiration if you have your own persistence mechanism (such as Realm, CoreData, etc).
///
/// ``DiskStorageEngine`` takes `Data` and saves it to disk using file system operations.
/// ``SQLiteStorageEngine`` takes `Data` and saves it to an SQLite database under the hood.
///
/// If you have your own way of storing data then you can refer to ``DiskStorageEngine`` and ``SQLiteStorageEngine``
/// for inspiration, but all you need to do is conform to the ``StorageEngine`` protocol
/// and initialize ``ObjectStorage`` with that storage.
public protocol StorageEngine<Key, Value>: Actor {
    associatedtype Key: StorageKey
    associatedtype Value
    
    /// Write a single value to the engine associated with the provided key
    ///
    /// If a value already exists for this key, the new value will replace the old value
    ///
    /// - Parameters:
    ///   - value: The `Value` to be written
    ///   - key: The `Key` that identifies the value
    /// - throws: May throw an error if the engine cannot write the value
    func write(_ value: Value, key: Key) async throws
    
    /// Write multiple keys and values to the engine
    ///
    /// A default implementation is provided that iteratively calls `write(_:key:)`
    /// - Parameter keysAndValues: The array of keys and values to be written
    /// - throws: May throw an error if any of the write attempts fails
    func write(_ keysAndValues: [(key: Key, value: Value)]) async throws

    func read(key: Key) async throws -> Value?
    func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Value)]
    func readAllValues() async throws -> [Value]
    func readAllKeysAndValues() async throws -> [(key: Key, value: Value)]

    func remove(key: Key) async throws
    func remove(keys: [Key]) async throws
    func removeAllValues() async throws

    func keyExists(_ key: Key) async throws -> Bool
    func keysExist(_ keys: [Key]) async throws -> [Key]
    func keyCount() async throws -> Int
    func allKeys() async throws -> [Key]

    func createdAt(key: Key) async throws -> Date?
    func updatedAt(key: Key) async throws -> Date?
}

public protocol StorageKey: Hashable, Sendable { 
    associatedtype KeyType: Hashable & Sendable
    
    var rawKey: KeyType { get }
    
    init(rawKey: KeyType)
}

// These default implementations make it easier to implement the `StorageEngine` protocol.
// Some `StorageEngine`s such as ``SQLiteStorageEngine`` may want to implement the one-item
// and array-based functions separately for optimization purposes, but these are safe defaults.
extension StorageEngine {
    
    /// Write many keys and values at once
    /// - Parameter keysAndValues: The array of `Key` and `Value` pairs to write
    public func write(_ keysAndValues: [(key: Key, value: Value)]) async throws {
        for (key, value) in keysAndValues {
            try await self.write(value, key: key)
        }
    }
    
    public func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Value)] {
        var result: [(Key, Value)] = []
        for key in keys {
            if let value = try await self.read(key: key) {
                result.append((key, value))
            }
        }
        return result
    }
    
    /// Reads all the `[Data]` located in the ``StorageEngine``.
    /// - Returns: An array of the `[Data]` contained in a ``StorageEngine``.
    public func readAllValues() async throws -> [Value] {
        return try await self.readAllKeysAndValues().map(\.value)
    }

    /// Reads all the `Data` located in the ``StorageEngine`` and returns an array
    /// of `[(Key, Value)]` tuples associated with the ``Key``.
    ///
    /// This method returns the ``Key`` and `Value` together in an array of `[(Key, Value)]`
    /// allowing you to know which ``Key`` led to a specific `Value` item being retrieved.
    /// This can be useful in allowing manual iteration over `Value` items, but if you don't need
    /// to know which ``Key`` led to a piece of `Value` being retrieved
    /// you can use ``readAllValues()`` instead.
    /// - Returns: An array of the `[Value]` and it's associated `Key`s contained in a directory.
    public func readAllKeysAndValues() async throws -> [(key: Key, value: Value)] {
        let allKeys = try await self.allKeys()
        var valuesAndKeys: [(key: Key, value: Value)] = []
        for key in allKeys {
            if let value = try await self.read(key: key) {
                valuesAndKeys.append((key, value))
            }
        }
        return valuesAndKeys
    }
    
    /// Removes `Value` items based on the associated array of `Key`s provided as a parameter.
    /// - Parameters:
    ///   - keys: A `[Key]` for matching multiple `Value` items.
    public func remove(keys: [Key]) async throws {
        for key in keys {
            try await self.remove(key: key)
        }
    }

    public func removeAllValues() async throws {
        let keys = try await self.allKeys()
        for key in keys {
            try await self.remove(key: key)
        }
    }
    
    /// Filters the provided keys to return only the ones that exist in the engine
    /// - Parameter keys: The list of keys to check for existence.
    /// - Returns: An array of keys that exist. This value is always a subset of the `keys` passed in.
    public func keysExist(_ keys: [Key]) async throws -> [Key] {
        let allKeys = try await self.allKeys()
        let keySet = Set(allKeys)
        return keys.filter({ keySet.contains($0) })
    }
    
    /// Read the number of keys located in the ``StorageEngine``.
    /// - Returns: The number of keys located in the ``StorageEngine``
    public func keyCount() async throws -> Int {
        return try await self.allKeys().count
    }
}
