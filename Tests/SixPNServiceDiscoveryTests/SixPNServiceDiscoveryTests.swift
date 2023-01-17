import XCTest
import NIO
@testable import SixPNServiceDiscovery

final class SixPNServiceDiscoveryTests: XCTestCase {
    var eventLoopGroup: MultiThreadedEventLoopGroup!
    var dnsClient: TestDNSClient!
    
    override func setUp() async throws {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        dnsClient = .unimplemented(on: eventLoopGroup.any())
    }
    
    struct TestError: Error {}
    
    func testRegisterAndLookup() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1", "s2"])
        }
        dnsClient.implementation.topNClosestInstances = { service in
            let appName = service.appName
            switch appName {
            case "s1":
                return eventLoop.makeSucceededFuture([try! .init(ipAddress: "::1", port: 80)])
            case "s2":
                return eventLoop.makeSucceededFuture([try! .init(ipAddress: "::2", port: 81)])
            default:
                return eventLoop.makeFailedFuture(TestError())
            }
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        let service2 = SixPNService(appName: "s2", port: 81)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .seconds(1),
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1, service2]).wait()
        )
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        let promise2 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        serviceDiscovery.lookup(service1) {
            promise1.succeed($0)
        }
        serviceDiscovery.lookup(service2) {
            promise2.succeed($0)
        }
        
        let serviceAddrs1 = try! promise1.futureResult.wait().get()
        let serviceAddrs2 = try! promise2.futureResult.wait().get()
        
        XCTAssertEqual(serviceAddrs1, [try! .init(ipAddress: "::1", port: 80)])
        XCTAssertEqual(serviceAddrs2, [try! .init(ipAddress: "::2", port: 81)])
        
        
    }
}
