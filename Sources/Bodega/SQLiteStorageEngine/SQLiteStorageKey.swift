import Foundation
import SQLite

public typealias SQLiteKeyType = SQLite.Value & Hashable & Sendable

public struct SQLiteStorageKey<Value: SQLiteKeyType>: StorageKey where Value.Datatype: Equatable {
    
    public let rawKey: Value
    
    public init(rawKey: Value) {
        self.rawKey = rawKey
    }
    
}

extension SQLiteStorageKey: ExpressibleByStringLiteral, ExpressibleByUnicodeScalarLiteral, ExpressibleByExtendedGraphemeClusterLiteral where Value == String {
    
    public init(stringLiteral value: String) {
        self.init(rawKey: value)
    }
    
}

extension SQLiteStorageKey: ExpressibleByIntegerLiteral where Value == Int {
    
    public init(integerLiteral value: Int) {
        self.init(rawKey: value)
    }
    
}
