import Foundation
import SQLite

public typealias SQLiteKeyType = SQLite.Value & Hashable & Sendable

public struct SQLiteStorageKey<Value: SQLiteKeyType>: StorageKey where Value.Datatype: Equatable {
    
    public let value: Value
    
    public init(value: Value) {
        self.value = value
    }
    
}

extension SQLiteStorageKey: ExpressibleByStringLiteral, ExpressibleByUnicodeScalarLiteral, ExpressibleByExtendedGraphemeClusterLiteral where Value == String {
    
    public init(stringLiteral value: String) {
        self.init(value: value)
    }
    
}

extension SQLiteStorageKey: ExpressibleByIntegerLiteral where Value == Int {
    
    public init(integerLiteral value: Int) {
        self.init(value: value)
    }
    
}
