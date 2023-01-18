import XCTest
import NIO
import Atomics
import ServiceDiscovery
@testable import SixPNServiceDiscovery

final class SixPNServiceDiscoveryTests: XCTestCase {
    var eventLoopGroup: MultiThreadedEventLoopGroup!
    var dnsClient: TestDNSClient!
    
    override func setUp() {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        dnsClient = .unimplemented(on: eventLoopGroup.any())
    }
    
    override func tearDown() {
        try! eventLoopGroup.syncShutdownGracefully()
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
            XCTFail("Should have thrown error")
        } catch {
            XCTAssertTrue(error is TestError)
        }
    }
    
    func testLookupTimeoutWithDefault() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        dnsClient.implementation.topNClosestInstances = { _ in
            // Simulate long-running future
            return eventLoop.scheduleTask(in: .seconds(1)) {
                return []
            }.futureResult
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .milliseconds(100),
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
        
        XCTAssertThrowsError(try promise1.futureResult.wait().get(), "Lookup should have timed out and returned LookupError.timedOut") { error in
            XCTAssertEqual(error as? LookupError, LookupError.timedOut)
        }
    }
    
    func testLookupTimeoutWithCustomDeadline() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        dnsClient.implementation.topNClosestInstances = { _ in
            return eventLoop.scheduleTask(in: .seconds(1)) {
                return []
            }.futureResult
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .seconds(2), // Set long default timeout
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        serviceDiscovery.lookup(
            service1,
            deadline: .now() + .milliseconds(10) // Override long default timeout
        ) {
            promise1.succeed($0)
        }
        
        XCTAssertThrowsError(try promise1.futureResult.wait().get(), "Lookup should have timed out and returned LookupError.timedOut") { error in
            XCTAssertEqual(error as? LookupError, LookupError.timedOut)
        }
    }
    
    func testLookupShutdown() throws {
        let eventLoop = eventLoopGroup.next()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        dnsClient.implementation.topNClosestInstances = { _ in
            return eventLoop.makeSucceededFuture([])
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .minutes(1),
            on: eventLoop
        )
        
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        try! serviceDiscovery.shutdown().wait()
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        serviceDiscovery.lookup(service1) {
            promise1.succeed($0)
        }
        
        XCTAssertThrowsError(try promise1.futureResult.wait().get(), "Lookup should have returned ServiceDiscoveryError.unavailable") { error in
            XCTAssertEqual(error as? ServiceDiscoveryError, ServiceDiscoveryError.unavailable)
        }
    }
    
    func testSubscribe() throws {
        let eventLoop = EmbeddedEventLoop()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }
        
        let accessCount = ManagedAtomic<Int>(1)
        dnsClient.implementation.topNClosestInstances = { service in
            let access = accessCount.load(ordering: .acquiring)
            defer { accessCount.wrappingIncrement(ordering: .relaxed) }
            
            XCTAssertEqual(service.appName, "s1")
            
            // On every call, we change the IP address to simulate returning new instances every time
            let ip = "::\(access)"
            return eventLoop.makeSucceededFuture([try! .init(ipAddress: ip, port: 80)])
        }
        dnsClient.implementation.close = {}
        
        let service1 = SixPNService(appName: "s1", port: 80)
        
        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .minutes(1),
            updateInterval: .seconds(1), // Checks for instance updates every second
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        eventLoop.advanceTime(by: .milliseconds(100)) // Advance time to let findAndRegister() work complete
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        let promise2 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        
        let updateCount = ManagedAtomic<Int>(0)
        let cancellationToken = serviceDiscovery.subscribe(to: service1) {
            let count = updateCount.load(ordering: .acquiring)
            defer { updateCount.wrappingIncrement(ordering: .relaxed) }
            switch count {
            case 0: promise1.succeed($0)
            case 1: promise2.succeed($0)
            default: XCTFail("nextResultHandler called too many times")
            }
        }
        
        XCTAssertFalse(cancellationToken.isCancelled)
        
        guard let firstInstances = try? promise1.futureResult.wait().get() else {
            XCTFail("First batch of instances of subscription returned an error")
            return
        }
        XCTAssertEqual(firstInstances, [try! .init(ipAddress: "::2", port: 80)])
        
        eventLoop.advanceTime(by: .milliseconds(1500)) // Advance time by 1.5 seconds, enough time for an instance update round to complete
        
        guard let secondInstances = try? promise2.futureResult.wait().get() else {
            XCTFail("Second batch of instances of subscription returned an error")
            return
        }
        XCTAssertEqual(secondInstances, [try! .init(ipAddress: "::3", port: 80)])
        
    }
    
    func testSubscribeWithContinuedInstanceError() throws {
        let eventLoop = EmbeddedEventLoop()
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
            updateInterval: .seconds(1),
            on: eventLoop
        )
        defer { try! serviceDiscovery.shutdown().wait() }
        
        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )
        
        eventLoop.advanceTime(by: .milliseconds(100)) // Advance time to let findAndRegister() work complete
        
        let promise1 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        let promise2 = eventLoop.makePromise(of: Result<[SocketAddress], Error>.self)
        let onCompletionPromise = eventLoop.makePromise(of: CompletionReason.self)
        
        let updateCount = ManagedAtomic<Int>(0)
        let cancellationToken = serviceDiscovery.subscribe(to: service1) { updateResult in
            let count = updateCount.load(ordering: .acquiring)
            defer { updateCount.wrappingIncrement(ordering: .relaxed) }
            switch count {
            case 0: promise1.succeed(updateResult)
            case 1: promise2.succeed(updateResult)
            default: XCTFail("nextResultHandler called too many times")
            }
        } onComplete: { completionReason in
            onCompletionPromise.succeed(completionReason)
        }
        
        guard case let .failure(error1) = try? promise1.futureResult.wait() else {
            XCTFail("nextResultHandler should have returned an error")
            return
        }
        
        XCTAssertTrue(error1 is TestError)
        
        eventLoop.advanceTime(by: .milliseconds(1500)) // Advance time by 1.5 seconds, enough time for an instance update round to complete
        
        guard case let .failure(error2) = try? promise2.futureResult.wait() else {
            XCTFail("nextResultHandler should have returned an error")
            return
        }
        XCTAssertTrue(error2 is TestError)
        
        XCTAssertFalse(cancellationToken.isCancelled)
        
        cancellationToken.cancel()
        
        guard let completionReason = try? onCompletionPromise.futureResult.wait() else {
            XCTFail("Should have returned completion reason")
            return
        }
        
        XCTAssertEqual(completionReason, .cancellationRequested)
        XCTAssertTrue(cancellationToken.isCancelled)
        
    }
    
    func testSubscribeShutdown() throws {
        let eventLoop = EmbeddedEventLoop()
        dnsClient.implementation.allSixPNApps = {
            eventLoop.makeSucceededFuture(["s1"])
        }

        dnsClient.implementation.topNClosestInstances = { _ in
                eventLoop.makeSucceededFuture([])
        }
        dnsClient.implementation.close = {}

        let service1 = SixPNService(appName: "s1", port: 80)

        let serviceDiscovery = SixPNServiceDiscovery(
            dnsClient: dnsClient,
            defaultLookupTimeout: .seconds(1),
            on: eventLoop
        )


        XCTAssertNoThrow(
            try serviceDiscovery.findAndRegister(services: [service1]).wait()
        )

        try! serviceDiscovery.shutdown().wait()

        eventLoop.advanceTime(by: .milliseconds(100))

        let onCompletionPromise = eventLoop.makePromise(of: CompletionReason.self)

        let cancellationToken = serviceDiscovery.subscribe(to: service1) { updateResult in
            XCTFail("nextResultHandler should not have been called")
        } onComplete: { completionReason in
            onCompletionPromise.succeed(completionReason)
        }

        guard let completionReason = try? onCompletionPromise.futureResult.wait() else {
            XCTFail("Failed getting completion reason")
            return
        }

        XCTAssertEqual(completionReason, .serviceDiscoveryUnavailable)
        XCTAssertTrue(cancellationToken.isCancelled)
    }
}
