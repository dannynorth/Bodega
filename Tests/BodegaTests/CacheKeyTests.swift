import XCTest
@testable import Bodega

final class CacheKeyTests: XCTestCase {
    func testCacheKeyValues() {
        let cacheKey = Legacy.CacheKey(verbatim: "cache-key")
        let hashedCacheKey = Legacy.CacheKey("cache-key")
        let hashedCacheKeyValue = "2536A137-81F3-3E55-574F-AFF9ACB9F995"

        XCTAssertEqual(hashedCacheKey.value, hashedCacheKeyValue)
        XCTAssertEqual(cacheKey.value, "cache-key")
    }

    func testCacheKeyURLHashing() {
        let redPandaClubHash = "37E97C2D-25C0-19AE-755D-FC39211AEE32"
        let url = URL(string: "https://www.redpanda.club")!
        let cacheKey = Legacy.CacheKey(url: url)

        XCTAssertEqual(cacheKey.value, redPandaClubHash)

        let dummyHash = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        XCTAssertNotEqual(cacheKey.value, dummyHash)
    }

    func testCacheKeyRawValue() {
        let urlString = "https://www.redpanda.club"
        let url = URL(string: urlString)!

        let urlInitializedCacheKey = Legacy.CacheKey(url: url)
        let stringInitializedCacheKey = Legacy.CacheKey(urlString)
        let verbatimInitializedCacheKey = Legacy.CacheKey(verbatim: urlString)

        XCTAssertEqual(urlInitializedCacheKey.rawValue, urlString)
        XCTAssertEqual(stringInitializedCacheKey.rawValue, urlString)
        XCTAssertEqual(verbatimInitializedCacheKey.rawValue, urlString)
    }
}
