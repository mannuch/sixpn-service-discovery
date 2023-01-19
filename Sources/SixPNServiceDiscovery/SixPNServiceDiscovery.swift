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
    
    private let _defaultLookupTimeout: TimeAmount
    private let updateInterval: TimeAmount
    private let cleanupInterval: TimeAmount
    
    private let logger: Logger
    
    private let dnsClient: any SixPNDNSClient
    
    private let eventLoop: EventLoop
    
    private var services: [Service] = []
    private var subscriptions: [Service: [Subscription]] = [:]
    
    private let _isShutdown = ManagedAtomic<Bool>(false)
    
    public var isShutdown: Bool {
        self._isShutdown.load(ordering: .acquiring)
    }
    
    public var defaultLookupTimeout: DispatchTimeInterval {
        .nanoseconds(Int(_defaultLookupTimeout.nanoseconds))
    }
    
    public init(
        dnsClient: any SixPNDNSClient,
        defaultLookupTimeout: TimeAmount,
        updateInterval: TimeAmount = .minutes(1),
        cleanupInterval: TimeAmount = .minutes(1),
        logger: Logger = .init(label: "swift-service-discovery.sixpn"),
        on eventLoop: EventLoop
    ) {
        self._defaultLookupTimeout = defaultLookupTimeout
        self.updateInterval = updateInterval
        self.cleanupInterval = cleanupInterval
        self.dnsClient = dnsClient
        self.logger = logger
        self.eventLoop = eventLoop
    }
    
    public func lookup(_ service: Service, deadline: DispatchTime? = nil, callback: @escaping (Result<[SocketAddress], Error>) -> Void) {
        guard !self.isShutdown else {
            callback(.failure(ServiceDiscoveryError.unavailable))
            return
        }
        
        let promise = self.eventLoop.makePromise(of: [SocketAddress].self)
        
        self.logger.debug("Looking up '\(service.appName)' instances")
        
        var timeout: TimeAmount?
        if let deadline {
            timeout = DispatchTime.now().distance(to: deadline).toNIOTimeAmount()
        }
        
        let deadlineTask = self.eventLoop.scheduleTask(
            in: timeout ?? self._defaultLookupTimeout
        ) {
            promise.fail(LookupError.timedOut)
        }
        
        self.lookup(service)
            .cascade(to: promise)
        
        promise.futureResult
            .always { _ in deadlineTask.cancel() }
            .whenComplete { result in
                switch result {
                case .success(let instances):
                    self.logger.debug("Found '\(service.appName)' instances: '\(instances)'")
                case .failure(let error):
                    self.logger.debug("Error looking up '\(service.appName)' instances: '\(error)'")
                }
                callback(result)
            }
    }
    
    public func subscribe(
        to service: Service,
        onNext nextResultHandler: @escaping (Result<[SocketAddress], Error>) -> Void,
        onComplete completionHandler: @escaping (CompletionReason) -> Void = { _ in }
    ) -> CancellationToken {
        
        self.logger.debug("Subscribing to '\(service.appName)' instance updates")
        
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
        
        self.subscribe(to: service, with: subscription).whenComplete { result in
            switch result {
            case .success:
                self.logger.debug("Successfully subscribed to '\(service.appName)' instance updates")
            case .failure(let error):
                self.logger.debug("Error subscribing to '\(service.appName)': '\(error)'")
            }
        }
        
        return cancellationToken
    }
    
    public func findAndRegister(services: [SixPNService]) -> EventLoopFuture<Void> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.findAndRegister(services: services)
            }
        }
        
        guard !self.isShutdown else {
            return self.eventLoop.makeFailedFuture(ServiceDiscoveryError.unavailable)
        }
        
        
        logger.info("Querying DNS for all SixPN apps in Fly.io organization...")
        
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        self.dnsClient.getAllAppNames()
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
            
            // Register services
            services.forEach { service in
                self.services.append(service)
            }
            
            // Kick off repeated task that continually updates service instances
            self.updateServiceInstances()
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
        
        // Change _isShudown from false to true, but if already true just return
        guard self._isShutdown.compareExchange(expected: false, desired: true, ordering: .acquiring).exchanged else {
            return self.eventLoop.makeSucceededVoidFuture()
        }
        
        self.dnsClient.close()
        self.logger.trace("SixPNServiceDiscovery is shutting down, closing all active queries and subscriptions on this event loop")
        
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

// MARK: Private methods
extension SixPNServiceDiscovery {
    private func lookup(_ service: Service) -> EventLoopFuture<[SocketAddress]> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.lookup(service)
            }
        }
        
        guard self.services.contains(service) else {
            return self.eventLoop.makeFailedFuture(LookupError.unknownService)
        }
        
        return self.dnsClient.getTopNClosestInstances(of: service).hop(to: eventLoop)
    }
    
    private func subscribe(
        to service: Service,
        with subscription: Subscription
    ) -> EventLoopFuture<Void> {
        // dispatch to event loop thread if necessary
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.subscribe(to: service, with: subscription)
            }
        }
        
        var subscriptions = self.subscriptions.removeValue(forKey: service) ?? [Subscription]()
        subscriptions.append(subscription)
        self.subscriptions[service] = subscriptions
        
        
        self.lookup(service)
            .whenComplete(subscription.nextResultHandler)
            
        
        return self.eventLoop.makeSucceededVoidFuture()
    }
    
    
    private func updateServiceInstances() {
        // Schedule task to continually update instances
        self.eventLoop.scheduleRepeatedAsyncTask(initialDelay: .zero, delay: self.updateInterval) { [weak self, eventLoop = self.eventLoop] task in
            guard let self, !self.isShutdown else {
                task.cancel()
                return eventLoop.makeSucceededVoidFuture()
            }
            self.logger.info("Updating service instances...")
            let promise = eventLoop.makePromise(of: [(SixPNService, Result<[SocketAddress], Error>)].self)
            
            self.services.sequencedFlatMapEach(on: self.eventLoop) { service in
                self.logger.info("Looking for top \(service.topNClosestInstances) closest instances of service ['\(service.appName)', port: \(service.port)]")
                let lookupPromise = eventLoop.makePromise(of: [SocketAddress].self)
                // Each update should timeout after the default lookup timeout
                let timeoutTask = eventLoop.scheduleTask(in: self._defaultLookupTimeout, {
                    self.logger.warning("Updating instances of service ['\(service.appName)', port: \(service.port)] timed out")
                    lookupPromise.fail(ServiceDiscoveryError.other("Time out updating instances of service ['\(service.appName)', port: \(service.port)]"))
                })
                
                self.dnsClient.getTopNClosestInstances(of: service)
                    .hop(to: eventLoop)
                    .cascade(to: lookupPromise)
                
                return lookupPromise.futureResult
                    .always {_ in timeoutTask.cancel() }
                    .map { instances in (service, .success(instances)) } // Map successful lookups to (SixPNService, Result.success([SocketAddress]))
                    .recover { error in (service, .failure(error)) } // Map failed lookups to (SixPNService, Result.failure(Error))
            }
            .cascade(to: promise)
            
            promise.futureResult.whenSuccess {
                for (service, instanceUpdateResult) in $0 {
                    // Notify subscriptions of instance updates
                    if !self.isShutdown, let subscriptions = self.subscriptions[service] {
                        subscriptions
                            .filter { !$0.cancellationToken.isCancelled }
                            .forEach { $0.nextResultHandler(instanceUpdateResult) }
                    }
                }
            }
            
            return promise.futureResult.map { _ in }
        }
    }
    
    private func cleanupSubscriptions() {
        // Schedule task to continually clean up any cancelled subscriptions
        self.eventLoop.scheduleRepeatedTask(initialDelay: self.cleanupInterval, delay: self.cleanupInterval) { [weak self] task in
            guard let self, !self.isShutdown else {
                task.cancel()
                return
            }
            
            for service in self.subscriptions.keys {
                self.subscriptions[service]?.removeAll(where: {
                    $0.cancellationToken.isCancelled
                })
            }
        }
    }
}


// MARK: Async/Await APIs
extension SixPNServiceDiscovery {
    public func findAndRegister(services: [SixPNService]) async throws {
        return try await self.findAndRegister(services: services).get()
    }
}

// MARK: Sendable
extension SixPNServiceDiscovery: @unchecked Sendable {}


extension DispatchTimeInterval {
    func toNIOTimeAmount() -> TimeAmount? {
        switch self {
        case .seconds(let int):
            return .seconds(Int64(int))
        case .milliseconds(let int):
            return .milliseconds(Int64(int))
        case .microseconds(let int):
            return .microseconds(Int64(int))
        case .nanoseconds(let int):
            return .nanoseconds(Int64(int))
        case .never:
            return .zero
        default:
            return nil
        }
    }
}

extension Optional<Result<[SocketAddress], Error>> {
    func equals(_ other: Result<[SocketAddress], Error>?) -> Bool {
        switch (self, other) {
        case (.none, .some), (.some, .none):
            return false
        case (.some(let result), .some(let otherResult)):
            return result.equals(otherResult)
        case (.none, .none):
            return true
        }
    }
}

extension Result<[SocketAddress], Error> {
    func equals(_ other: Result<[SocketAddress], Error>) -> Bool {
        switch (self, other) {
        case (.success(let instances), .success(let otherInstances)):
            return instances == otherInstances
        case (.success, .failure), (.failure, .success):
            return false
        case (.failure, .failure):
            return false
        }
    }
    
}

