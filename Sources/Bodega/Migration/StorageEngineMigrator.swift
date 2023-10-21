import Foundation

public actor StorageEngineMigrator<Old: StorageEngine, New: StorageEngine> {
    
    public struct Options {
        
        public enum MergeResult {
            case keepExisting
            case replace(New.Value)
        }
        
        public var performSequentialMigration: Bool = true
        public var removeOldValues: Bool = true
        public var mergeExistingValues: (_ existing: New.Value, _ migrating: New.Value) -> MergeResult = { .replace($1) }
        
        public init() { }
        
    }
    
    private let old: Old
    private let new: New
    
    private let keyTranslator: (Old.Key) -> New.Key
    private let valueTranslator: (Old.Value) -> New.Value
    
    public init(from old: Old, to new: New, migrateKey: @escaping (Old.Key) -> New.Key, migrateValue: @escaping (Old.Value) -> New.Value) {
        self.old = old
        self.new = new
        self.keyTranslator = migrateKey
        self.valueTranslator = migrateValue
    }
    
    public init(from old: Old, to new: New) where Old.Key == New.Key, Old.Value == New.Value {
        self.init(from: old, to: new, migrateKey: { $0 }, migrateValue: { $0 })
    }
    
    public init(from old: Old, to new: New, migrateKey: @escaping (Old.Key) -> New.Key) where Old.Value == New.Value {
        self.init(from: old, to: new, migrateKey: migrateKey, migrateValue: { $0 })
    }
    
    public init(from old: Old, to new: New, migrateValue: @escaping (Old.Value) -> New.Value) where Old.Key == New.Key {
        self.init(from: old, to: new, migrateKey: { $0 }, migrateValue: migrateValue)
    }
    
    public init(from old: Old, to new: New) where Old.Key.KeyType == New.Key.KeyType, Old.Value == New.Value {
        self.init(from: old, to: new, migrateKey: { New.Key($0) }, migrateValue: { $0 })
    }
    
    public init(from old: Old, to new: New, migrateValue: @escaping (Old.Value) -> New.Value) where Old.Key.KeyType == New.Key.KeyType {
        self.init(from: old, to: new, migrateKey: { New.Key($0) }, migrateValue: migrateValue)
    }
    
    public func migrate(options: Options) async throws {
        if options.performSequentialMigration {
            try await self.performSequentialMigration(options: options)
        } else {
            try await self.performAllAtOnceMigration(options: options)
        }
        
    }
    
    private func performAllAtOnceMigration(options: Options) async throws {
        let allOldKeysAndValues = try await old.readAllKeysAndValues()
        let allExistingKeys = Set(try await new.allKeys())
        
        var newKeysAndValues: [(New.Key, New.Value)] = []
        
        for (oldKey, oldValue) in allOldKeysAndValues {
            let newKey = keyTranslator(oldKey)
            let newValue = valueTranslator(oldValue)
            if allExistingKeys.contains(newKey), let existingValue = try await new.read(key: newKey) {
                let mergeResult = options.mergeExistingValues(existingValue, newValue)
                switch mergeResult {
                    case .keepExisting:
                        break
                    case .replace(let newValue):
                        newKeysAndValues.append((newKey, newValue))
                }
            } else {
                newKeysAndValues.append((newKey, newValue))
            }
        }
        
        try await new.write(newKeysAndValues)
        
        if options.removeOldValues {
            try await old.removeAllValues()
        }
    }
    
    private func performSequentialMigration(options: Options) async throws {
        let allKeys = try await old.allKeys()
        
        for oldKey in allKeys {
            guard let oldValue = try await old.read(key: oldKey) else { continue }
            
            let newKey = keyTranslator(oldKey)
            let newValue = valueTranslator(oldValue)
            
            if let existing = try await new.read(key: newKey) {
                let mergeResult = options.mergeExistingValues(existing, newValue)
                switch mergeResult {
                    case .keepExisting:
                        break
                    case .replace(let newValue):
                        try await new.write(newValue, key: newKey)
                }
            } else {
                try await new.write(newValue, key: newKey)
            }
            
            if options.removeOldValues == true {
                try await old.remove(key: oldKey)
            }
        }
    }
}
