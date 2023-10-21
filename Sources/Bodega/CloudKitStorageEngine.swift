import Foundation
import CloudKit

public actor CloudKitStorageEngine<Key: StorageKey>: StorageEngine where Key.KeyType == String {
    
    public typealias Value = Data
    
    private let database: CKDatabase
    private let zone: CKRecordZone?
    private let recordType: CKRecord.RecordType
    
    private var dataKey: CKRecord.FieldKey { "data" }
    
    public init(database: CKDatabase, zone: CKRecordZone? = nil, recordType: CKRecord.RecordType) {
        self.database = database
        self.zone = zone
        self.recordType = recordType
    }
    
    public func write(_ value: Data, key: Key) async throws {
        try await self.write([(key, value)])
    }
    
    public func write(_ keysAndValues: [(key: Key, value: Data)]) async throws {
        var recordsToSave: [CKRecord] = []
        
        for (key, value) in keysAndValues {
            let record = CKRecord(recordType: recordType, recordID: self.recordID(for: key))
            record[dataKey] = value
            
            recordsToSave.append(record)
        }
        
        try await self.write(records: recordsToSave)
    }
    
    public func read(key: Key) async throws -> Data? {
        let id = self.recordID(for: key)
        let response = try await self.fetch(recordIDs: [id])
        guard let record = response[id] else { return nil }
        return record[dataKey]
    }
    
    public func readKeysAndValues(keys: [Key]) async throws -> [(key: Key, value: Data)] {
        let ids = keys.map { self.recordID(for: $0) }
        let response = try await self.fetch(recordIDs: ids)
        
        return keys.compactMap { key -> (Key, Data)? in
            let id = self.recordID(for: key)
            guard let record = response[id] else { return nil }
            guard let data = record[dataKey] as? Data else { return nil }
            return (key, data)
        }
    }
    
    public func readAllValues() async throws -> [Data] {
        let everything = try await self.fetch(recordIDs: nil)
        return everything.values.compactMap { record -> Data? in
            record[dataKey]
        }
    }
    
    public func readAllKeysAndValues() async throws -> [(key: Key, value: Data)] {
        let everything = try await self.fetch(recordIDs: nil)
        return try everything.compactMap { (id, record) -> (Key, Data)? in
            guard let data = record[dataKey] as? Data else { return nil }
            
            let key = try self.key(for: id)
            return (key, data)
        }
    }
    
    public func remove(key: Key) async throws {
        let id = self.recordID(for: key)
        try await self.delete(recordIDs: [id])
    }
    
    public func remove(keys: [Key]) async throws {
        let ids = keys.map { self.recordID(for: $0) }
        try await self.delete(recordIDs: ids)
    }
    
    public func removeAllValues() async throws {
        let allIDs = try await self.allRecordIDs()
        try await self.delete(recordIDs: allIDs)
    }
    
    public func keyExists(_ key: Key) async throws -> Bool {
        let id = self.recordID(for: key)
        let response = try await self.fetch(recordIDs: [id], fields: .systemFields)
        return response[id] != nil
    }
    
    public func keysExist(_ keys: [Key]) async throws -> [Key] {
        let ids = keys.map { self.recordID(for: $0) }
        let response = try await self.fetch(recordIDs: ids, fields: .systemFields)
        
        return keys.filter { key in
            let id = self.recordID(for: key)
            return response[id] != nil
        }
    }
    
    public func keyCount() async throws -> Int {
        let allIDs = try await self.allRecordIDs()
        return allIDs.count
    }
    
    public func allKeys() async throws -> [Key] {
        let allIDs = try await self.allRecordIDs()
        return try allIDs.map { try self.key(for: $0) }
    }
    
    public func createdAt(key: Key) async throws -> Date? {
        let id = self.recordID(for: key)
        let response = try await self.fetch(recordIDs: [id], fields: .systemFields)
        let record = response[id]
        return record?.creationDate
    }
    
    public func updatedAt(key: Key) async throws -> Date? {
        let id = self.recordID(for: key)
        let response = try await self.fetch(recordIDs: [id], fields: .systemFields)
        let record = response[id]
        return record?.modificationDate
    }
    
    // MARK: - CloudKit operations
    
    private enum DesiredFields {
        case systemFields
        case allFields
        case specificFields([CKRecord.FieldKey])
        
        var fieldsForOperations: [CKRecord.FieldKey]? {
            switch self {
                case .systemFields: return []
                case .allFields: return nil
                case .specificFields(let fields): return fields
            }
        }
    }
    
    private func allRecordIDs() async throws -> [CKRecord.ID] {
        let everything = try await self.fetch(recordIDs: nil, fields: .systemFields)
        return Array(everything.keys)
        
    }
    
    private func recordID(for key: Key) -> CKRecord.ID {
        let recordID: CKRecord.ID
        if let zone {
            recordID = CKRecord.ID(recordName: key.rawKey, zoneID: zone.zoneID)
        } else {
            recordID = CKRecord.ID(recordName: key.rawKey)
        }
        return recordID
    }
    
    private func key(for recordID: CKRecord.ID) throws -> Key {
        return try Key(rawKey: recordID.recordName)
    }
    
    private func write(records: [CKRecord]) async throws {
        var currentStart = records.startIndex
        let chunkSize = 50
        
        while currentStart < records.endIndex {
            let sliceEnd = min(currentStart + chunkSize, records.endIndex)
            let slice = Array(records[currentStart ..< sliceEnd])
            
            if #available(macOS 12, *) {
                let (saveResults, _) = try await database.modifyRecords(saving: slice, deleting: [], savePolicy: .changedKeys, atomically: true)
                for record in records {
                    // make sure everything succeeded
                    guard let result = saveResults[record.recordID] else {
                        throw CKError(.partialFailure)
                    }
                    
                    if case .failure(let error) = result {
                        throw error
                    }
                }
            } else {
                let operation = CKModifyRecordsOperation(recordsToSave: slice, recordIDsToDelete: nil)
                operation.savePolicy = .changedKeys
                
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    operation.modifyRecordsCompletionBlock = { _, _, error in
                        if let error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume()
                        }
                    }
                    
                    self.database.add(operation)
                }
            }
            
            currentStart = sliceEnd
        }
    }
    
    private func delete(recordIDs: [CKRecord.ID]) async throws {
        var currentStart = recordIDs.startIndex
        let chunkSize = 50
        
        while currentStart < recordIDs.endIndex {
            let sliceEnd = min(currentStart + chunkSize, recordIDs.endIndex)
            let slice = Array(recordIDs[currentStart ..< sliceEnd])
            
            if #available(macOS 12, *) {
                let (_, results) = try await database.modifyRecords(saving: [], deleting: slice, savePolicy: .allKeys, atomically: false)
                for result in results.values {
                    if case .failure(let error) = result {
                        throw error
                    }
                }
            } else {
                let operation = CKModifyRecordsOperation(recordsToSave: nil, recordIDsToDelete: slice)
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                    operation.modifyRecordsCompletionBlock = { _, _, error in
                        if let error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume()
                        }
                    }
                    self.database.add(operation)
                }
            }
            
            currentStart = sliceEnd
        }
    }
    
    // nil = everything
    private func fetch(recordIDs: [CKRecord.ID]?, fields: DesiredFields = .systemFields) async throws -> [CKRecord.ID: CKRecord] {
        if let recordIDs {
            return try await self.fetchRecords(ids: recordIDs, fields: fields)
        } else {
            return try await self.fetchEverything(fields: fields)
        }
    }
    
    private func fetchRecords(ids: [CKRecord.ID], fields: DesiredFields) async throws -> [CKRecord.ID: CKRecord] {
        var results: [CKRecord.ID: CKRecord] = [:]
        
        var currentStart = ids.startIndex
        let chunkSize = 50
        
        while currentStart < ids.endIndex {
            let sliceEnd = min(currentStart + chunkSize, ids.endIndex)
            let slice = Array(ids[currentStart ..< sliceEnd])
            
            if #available(macOS 12, *) {
                let response = try await database.records(for: slice, desiredKeys: fields.fieldsForOperations)
                
                for (id, recordResult) in response {
                    switch recordResult {
                        case .success(let record):
                            results[id] = record
                        case .failure(let error):
                            throw error
                    }
                }
            } else {
                let operation = CKFetchRecordsOperation(recordIDs: slice)
                operation.desiredKeys = fields.fieldsForOperations
                
                let response: [CKRecord.ID: CKRecord] = try await withCheckedThrowingContinuation { continuation in
                    operation.fetchRecordsCompletionBlock = { records, error in
                        if let error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume(returning: records ?? [:])
                        }
                    }
                }
                
                for (id, record) in response {
                    results[id] = record
                }
            }
            
            currentStart = sliceEnd
        }
        return [:]
    }
    
    private func fetchEverything(fields: DesiredFields) async throws -> [CKRecord.ID: CKRecord] {
        var results: [CKRecord.ID: CKRecord] = [:]
        let query = CKQuery(recordType: self.recordType, predicate: NSPredicate(value: true))
        
        var nextPage: CKQueryOperation.Cursor?
        repeat {
            let response: [(CKRecord.ID, Result<CKRecord, Error>)]
            let cursor: CKQueryOperation.Cursor?
            
            if #available(macOS 12, *) {
                if let thisPage = nextPage {
                    (response, cursor) = try await database.records(continuingMatchFrom: thisPage, desiredKeys: fields.fieldsForOperations)
                } else {
                    (response, cursor) = try await database.records(matching: query, inZoneWith: self.zone?.zoneID, desiredKeys: fields.fieldsForOperations)
                }
            } else {
                let operation: CKQueryOperation
                if let nextPage {
                    operation = CKQueryOperation(cursor: nextPage)
                } else {
                    operation = CKQueryOperation(query: query)
                }
                operation.desiredKeys = fields.fieldsForOperations
                operation.zoneID = self.zone?.zoneID
                
                let result: ([(CKRecord.ID, Result<CKRecord, Error>)], CKQueryOperation.Cursor?) = try await withCheckedThrowingContinuation { continuation in
                    var inProgressResults: [(CKRecord.ID, Result<CKRecord, Error>)] = []
                    operation.recordFetchedBlock = {
                        inProgressResults.append(($0.recordID, .success($0)))
                    }
                    operation.queryCompletionBlock = { cursor, error in
                        if let error {
                            continuation.resume(throwing: error)
                        } else {
                            continuation.resume(returning: (inProgressResults, cursor))
                        }
                    }
                    
                    self.database.add(operation)
                }
                
                (response, cursor) = result
            }
            
            for (recordID, result) in response {
                switch result {
                    case .success(let record):
                        results[recordID] = record
                    case .failure(let error):
                        throw error
                }
            }
            
            nextPage = cursor
        } while nextPage != nil
        
        return results
    }
    
}
