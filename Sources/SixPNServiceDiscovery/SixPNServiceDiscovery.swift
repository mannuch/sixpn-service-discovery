import Dispatch
import DNSClient
import ServiceDiscovery
import NIO
import Atomics
import Logging
import AsyncKit

public final class SixPNServiceDiscovery: ServiceDiscovery {
    public typealias Service = SixPNService
    public typealias Instance = SocketAddress
    
    let _defaultLookupTimeout: TimeAmount
    
    let logger: Logger
    
    private let dnsClient: any SixPNDNSClient
    
    private let eventLoop: EventLoop
    
    private var services: [Service: [Instance]] = [:]
    private var subscriptions: [Service: [Subscription]] = [:]
    
    private var isShutdown: Bool = false
    
    public var defaultLookupTimeout: DispatchTimeInterval {
        .nanoseconds(Int(_defaultLookupTimeout.nanoseconds))
    }
    
    public init(
        dnsClient: any SixPNDNSClient,
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
        
        
        let deadlineTask = self.eventLoop.scheduleTask(
            deadline: deadline?.toNIODeadline() ?? .now() + self._defaultLookupTimeout
        ) {
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
            .whenComplete { result in
                switch result {
                case .success:
                    self.lookup(service, callback: nextResultHandler)
                case .failure:
                    cancellationToken.cancel()
                }
            }
        
        
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
        
        guard !instances.isEmpty else {
            // Instances is empty, so try doing a just-in-time DNS lookup of any instances, adding them to the self.services map if found
            let queryFuture = self.dnsClient.topNClosestInstances(of: service)
                .hop(to: eventLoop)
                .recover { _ in [] }
            queryFuture.whenSuccess { instances in
                guard !instances.isEmpty else { return }
                self.services[service] = instances
            }
            return queryFuture
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
        
        
        logger.info("Querying DNS for all SixPN apps in Fly.io organization...")
        
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        self.dnsClient.allSixPNApps()
            .hop(to: self.eventLoop)
            .flatMapThrowing { appNames in
                var notFound = [SixPNService]()
                
                services.forEach { service in
                    if !appNames.contains(service.appName) {
                        notFound.append(service)
                    }
                }
                
                guard notFound.isEmpty else {
                    self.logger.error("The following service apps were NOT found in SixPN network: \(notFound)")
                    throw SixPNError.servicesNotFound(notFound)
                }
            }.cascade(to: promise)
        
        promise.futureResult.whenSuccess {
            self.logger.info("Found all service apps in SixPN network")
            
            // Initialize registered services
            services.forEach { service in
                self.services[service] = []
            }
            
            // Kick off repeated task that continually updates service instances
            self.updateServiceInstances()
        }
        
        return promise.futureResult
    }
    
    private func updateServiceInstances() {
        // Schedule task to continually update instances
        self.eventLoop.scheduleRepeatedAsyncTask(initialDelay: .zero, delay: .minutes(1)) { [weak self, eventLoop = self.eventLoop] task in
            guard let self, !self.isShutdown else {
                task.cancel()
                return eventLoop.makeSucceededVoidFuture()
            }
            self.logger.info("Updating service instances...")
            let promise = eventLoop.makePromise(of: [(SixPNService, [SocketAddress])].self)
            
            self.services.keys.sequencedFlatMapEach(on: self.eventLoop) { service in
                self.logger.info("Looking for top \(service.topNClosestInstances) closest instances of service ['\(service.appName)', port: \(service.port)]")
                return self.dnsClient.topNClosestInstances(of: service)
                    .hop(to: eventLoop)
                    .map { instances in (service, instances) }
                    .recover { _ in
                        (service, [])
                    }
            }.cascade(to: promise)
            
            promise.futureResult.whenSuccess {
                for (service, instances) in $0 {
                    let prevInstances = self.services[service]
                    self.services[service] = instances
                    
                    if !self.isShutdown, prevInstances != instances, let subscription = self.subscriptions[service] {
                        subscription
                            .filter { !$0.cancellationToken.isCancelled }
                            .forEach { $0.nextResultHandler(.success(instances)) }
                    }
                }
            }
            
            return promise.futureResult.map { _ in }
        }
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
        self.dnsClient.close()
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

public struct SixPNService: Hashable, Equatable {
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
    
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        return lhs.appName == rhs.appName && lhs.port == rhs.port
    }
}

extension DispatchTime {
    func toNIODeadline() -> NIODeadline {
        NIODeadline.uptimeNanoseconds(self.uptimeNanoseconds)
    }
}

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

