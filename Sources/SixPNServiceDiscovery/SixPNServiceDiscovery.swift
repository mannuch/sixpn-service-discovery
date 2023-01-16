import Dispatch
import DNSClient
import ServiceDiscovery
import NIO
import Atomics
import Logging
import struct NIOConcurrencyHelpers.NIOLock

public final class SixPNServiceDiscovery: ServiceDiscovery {
    public typealias Service = SixPNService
    public typealias Instance = SocketAddress
    
    let _defaultLookupTimeout: TimeAmount
    
    let logger: Logger
    
    private let dnsClient: DNSClient
    
    private let eventLoop: EventLoop
    
    private var services: [Service: [Instance]] = [:]
    private var subscriptions: [Service: [Subscription]] = [:]
    
    private var isShutdown: Bool = false
    
    public var defaultLookupTimeout: DispatchTimeInterval {
        .nanoseconds(Int(_defaultLookupTimeout.nanoseconds))
    }
    
    public init(
        dnsClient: DNSClient,
        defaultLookupTimeout: TimeAmount,
        logger: Logger = .init(label: "swift-service-discovery.sixpn"),
        on eventLoop: EventLoop
    ) {
        self._defaultLookupTimeout = defaultLookupTimeout
        self.dnsClient = dnsClient
        self.logger = logger
        self.eventLoop = eventLoop
    }
    
    public func lookup(_ service: Service, deadline: DispatchTime? = nil, callback: @escaping (Result<[SocketAddress], Error>) -> Void) {
        let promise = self.eventLoop.makePromise(of: [SocketAddress].self)
        
        self.lookup(service, deadline: deadline?.toNIODeadline())
            .cascade(to: promise)
        
        
        let deadlineTask = self.eventLoop.scheduleTask(deadline: deadline?.toNIODeadline() ?? .now() + self._defaultLookupTimeout) {
            promise.fail(LookupError.timedOut)
        }
        
        promise.futureResult
            .always { _ in deadlineTask.cancel() }
            .whenComplete(callback)
    }
    
    public func subscribe(
        to service: Service,
        onNext nextResultHandler: @escaping (Result<[SocketAddress], Error>) -> Void,
        onComplete completionHandler: @escaping (CompletionReason) -> Void = { _ in }
    ) -> CancellationToken {
        guard !self.isShutdown else {
            completionHandler(.serviceDiscoveryUnavailable)
            return CancellationToken(isCancelled: true)
        }
        
        let cancellationToken = CancellationToken(completionHandler: completionHandler)
        let subscription = Subscription(
            nextResultHandler: nextResultHandler,
            completionHandler: completionHandler,
            cancellationToken: cancellationToken
        )
        
        saveSubscription(subscription, for: service)
            .flatMap {
                self.lookup(service)
            }
            .whenComplete(nextResultHandler)
        
        
        return cancellationToken
    }
    
    private func saveSubscription(_ subscription: Subscription, for service: Service) -> EventLoopFuture<Void> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.saveSubscription(subscription, for: service)
            }
        }
        
        var subscriptions = self.subscriptions.removeValue(forKey: service) ?? [Subscription]()
        subscriptions.append(subscription)
        self.subscriptions[service] = subscriptions
        
        return self.eventLoop.makeSucceededVoidFuture()
    }
    
    private func lookup(_ service: Service, deadline: NIODeadline? = nil) -> EventLoopFuture<[SocketAddress]> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.lookup(service, deadline: deadline)
            }
        }
        
        guard !self.isShutdown else {
            return self.eventLoop.makeFailedFuture(ServiceDiscoveryError.unavailable)
        }
        
        guard let instances = self.services[service] else {
            return self.eventLoop.makeFailedFuture(LookupError.unknownService)
        }
        
        return self.eventLoop.makeSucceededFuture(instances)
    }
    
    public func findAndRegister(services: [SixPNService]) -> EventLoopFuture<Void> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.findAndRegister(services: services)
            }
        }
        
        guard !self.isShutdown else {
            return self.eventLoop.makeFailedFuture(SixPNError.shutdown)
        }
        
        
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        self.checkSixPNExistence(services)
            .hop(to: self.eventLoop)
            .whenComplete { [weak self] result in
                if case let .failure(error) = result {
                    promise.fail(error)
                    return
                }
                
                switch result {
                case .success:
                    // Query AAAA records for socket addresses of services
                    guard let self else {
                        promise.succeed(())
                        return
                    }
                    var queries = [EventLoopFuture<Void>]()
                    for service in services {
                        queries.append(self.queryService(service))
                    }
                    EventLoopFuture.andAllSucceed(queries, promise: promise)
                    
                case let .failure(error):
                    promise.fail(error)
                }
            }
        
        return promise.futureResult
    }
    
    private func queryService(_ service: SixPNService) -> EventLoopFuture<Void> {
        self.logger.info("Looking for top \(service.topNClosestInstances) closest instances of service ['\(service.appName)', port: \(service.port)]")
        
        return self.dnsClient.initiateAAAAQuery(
            host: "top\(service.topNClosestInstances).nearest.of.\(service.appName).internal",
            port: Int(service.port)
        )
        .hop(to: self.eventLoop)
        .map { [weak self] socketAddresses in
            guard let self else { return }
            
            self.logger.info("Found \(socketAddresses.count) instances of service ['\(service.appName)', port: \(service.port)]")
            
            self.services[service] = socketAddresses
        }
    }
    
    private func checkSixPNExistence(_ services: [SixPNService]) -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        logger.info("Querying DNS for all SixPN apps in Fly.io organization...")
        
        promise.futureResult.whenComplete { [weak self] result in
            switch result {
            case .success:
                self?.logger.info("Found all queried service apps in SixPN network")
            case .failure(let error):
                self?.logger.error("Error querying service apps in SixPN network: '\(error)'")
            }
        }
        
        self.dnsClient.sendQuery(forHost: "_apps.internal", type: .txt).whenComplete { result in
            guard case let .success(message) = result else {
                if case let .failure(error) = result {
                    promise.fail(error)
                }
                return
            }
            
            
            guard !message.answers.isEmpty else {
                promise.fail(
                    SixPNError.txtRecordLookup(message: "The DNS Query message did not have any answers.")
                )
                return
            }
            
            guard case let .txt(txtRecord) = message.answers[0] else {
                promise.fail(
                    SixPNError.txtRecordLookup(message: "The first answer of the DNS Query message was not a TXT record.")
                )
                return
            }
            
            let appNames = txtRecord.resource.rawValues[0].split(separator: ",").map { String($0) }
            
            var notFound = [SixPNService]()
            
            services.forEach { service in
                if !appNames.contains(service.appName) {
                    notFound.append(service)
                }
            }
            
            guard notFound.isEmpty else {
                promise.fail(SixPNError.servicesNotFound(notFound))
                return
            }
            
            promise.succeed(())
        }
        
        return promise.futureResult
        
    }
    
    public func shutdown() -> EventLoopFuture<Void> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                return self.shutdown()
            }
        }
        
        // check to make sure we aren't double closing
        guard !self.isShutdown else {
            return self.eventLoop.makeSucceededFuture(())
        }
        
        self.isShutdown = true
        self.dnsClient.cancelQueries()
        self.logger.trace("SixPNServiceDiscovery is shutting down, closing all active queries and subscriptions on this event loop")
        
        // no locks needed as this can only happen once
        self.subscriptions.values.forEach { subscriptions in
            subscriptions
                .filter { !$0.cancellationToken.isCancelled }
                .forEach { $0.completionHandler(.serviceDiscoveryUnavailable) }
        }
        
        return self.eventLoop.makeSucceededVoidFuture()
    }
    
    deinit {
        if !self.isShutdown {
            assertionFailure("SixPNServiceDiscovery.shutdown() was not called before deinit.")
        }
    }
    
    private struct Subscription {
            let nextResultHandler: (Result<[Instance], Error>) -> Void
            let completionHandler: (CompletionReason) -> Void
            let cancellationToken: CancellationToken
    }
}

extension SixPNServiceDiscovery {
    public func findAndRegister(services: [SixPNService]) async throws {
        return try await self.findAndRegister(services: services).get()
    }
}

extension SixPNServiceDiscovery {
    public enum SixPNError: Error, CustomStringConvertible {
        case unknown
        case txtRecordLookup(message: String)
        case servicesNotFound([SixPNService])
        case shutdown
        
        public var description: String {
            switch self {
            case .unknown:
                return "Unknown"
            case .shutdown:
                return "SixPNServiceDiscovery is shutdown"
            case let .servicesNotFound(notFoundServices):
                return "Could not find services: \(notFoundServices.map { $0.description })"
            case let .txtRecordLookup(message):
                return "Error during TXT record DNS query: '\(message)'"
            }
        }
    }
}

public struct SixPNService: Hashable {
    public let appName: String
    public let port: UInt
    public let topNClosestInstances: UInt = 1
    
    public var description: String {
        "\(appName)"
    }
    
    public init(
        appName: String,
        port: UInt
    ) {
        self.appName = appName
        self.port = port
    }
}

extension DispatchTime {
    func toNIODeadline() -> NIODeadline {
        NIODeadline.uptimeNanoseconds(self.uptimeNanoseconds)
    }
}

