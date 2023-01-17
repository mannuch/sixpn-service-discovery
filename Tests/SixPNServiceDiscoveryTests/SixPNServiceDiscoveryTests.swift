import XCTest
import NIO
import Atomics
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
            switch service.appName {
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
    
    func testRegisterFailureServiceNotFound() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
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
        
        do {
            try serviceDiscovery.findAndRegister(services: [service1, service2]).wait()
            XCTAssert(true, "Failed to throw error")
        } catch SixPNError.servicesNotFound(let notFound) {
            XCTAssertEqual(notFound, [service2])
        } catch {
            XCTAssert(true, "Wrong error thrown")
        }
    }
    
    func testRegisterWithEmptyInstances() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1", "s2"])
        }
        dnsClient.implementation.topNClosestInstances = { service in
            eventLoop.makeSucceededFuture([])
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
    }
    
    func testRegisterWithEmptyInstancesWithLookup() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1", "s2"])
        }
        
        let accessCount = ManagedAtomic<Int>(0)
        dnsClient.implementation.topNClosestInstances = { service in
            // on first access, return no instances
            // on second access, return non-empty instances
            let access = accessCount.load(ordering: .acquiring)
            defer { accessCount.wrappingIncrement(ordering: .relaxed) }
            guard access > 0 else {
                return eventLoop.makeSucceededFuture([])
            }
            switch service.appName {
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
    
    func testRegisterWithInitialInstancesError() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        let accessCount = ManagedAtomic<Int>(0)
        dnsClient.implementation.topNClosestInstances = { service in
            // on first access, return no instances
            // on second access, return non-empty instances
            let access = accessCount.load(ordering: .acquiring)
            defer { accessCount.wrappingIncrement(ordering: .relaxed) }
            guard access > 0 else {
                return eventLoop.makeFailedFuture(TestError())
            }
            
            return eventLoop.makeSucceededFuture([try! .init(ipAddress: "::1", port: 80)])
            
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .seconds(1),
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        serviceDiscovery.lookup(service1) {
            promise1.succeed($0)
        }
        
        let serviceAddrs1 = try! promise1.futureResult.wait().get()
        
        XCTAssertEqual(serviceAddrs1, [try! .init(ipAddress: "::1", port: 80)])
    }
    
    func testContinuedInstancesError() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        dnsClient.implementation.topNClosestInstances = { _ in
                eventLoop.makeFailedFuture(TestError())
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .seconds(1),
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        serviceDiscovery.lookup(service1) {
            promise1.succeed($0)
        }
        
        do {
            _ = try promise1.futureResult.wait().get()
            XCTAssert(true, "Should have thrown error")
        } catch {
            XCTAssertTrue(error is TestError)
        }
    }
    
    func testLookupTimeout() throws {
        
    }
}
