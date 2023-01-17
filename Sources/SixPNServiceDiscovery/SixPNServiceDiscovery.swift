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
    
    private let logger: Logger
    
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
        
        self.logger.debug("Looking up '\(service.appName)' instances")
        
        self.lookup(service, deadline: deadline?.toNIODeadline())
            .cascade(to: promise)
        
        
        let deadlineTask = self.eventLoop.scheduleTask(
            deadline: deadline?.toNIODeadline() ?? .now() + self._defaultLookupTimeout
        ) {
            promise.fail(LookupError.timedOut)
        }
        
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
            return self.eventLoop.makeFailedFuture(SixPNError.shutdown)
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
            
            // Initialize registered services
            services.forEach { service in
                self.services[service] = []
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
        
        // check to make sure we aren't double closing
        guard !self.isShutdown else {
            return self.eventLoop.makeSucceededVoidFuture()
        }
        
        self.isShutdown = true
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
            self.logger.debug("No instances of '\(service.appName)' stored, querying DNS again...")
            let queryFuture = self.dnsClient.getTopNClosestInstances(of: service)
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
        
        guard !self.isShutdown else {
            subscription.cancellationToken.cancel()
            return self.eventLoop.makeSucceededVoidFuture()
        }
        
        
        var subscriptions = self.subscriptions.removeValue(forKey: service) ?? [Subscription]()
        subscriptions.append(subscription)
        self.subscriptions[service] = subscriptions
        
        
        self.lookup(service)
            .always(subscription.nextResultHandler)
            .whenFailure { _ in subscription.cancellationToken.cancel() }
            
        
        return self.eventLoop.makeSucceededVoidFuture()
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
                let lookupPromise = eventLoop.makePromise(of: (SixPNService, [SocketAddress]).self)
                // Each update should timeout after self._defaultLookupTimeout
                let timeoutTask = eventLoop.scheduleTask(in: self._defaultLookupTimeout, {
                    struct TimeoutError: Error {}
                    self.logger.debug("Updating instances of service ['\(service.appName)', port: \(service.port)] timed out")
                    lookupPromise.fail(TimeoutError())
                })
                
                self.dnsClient.getTopNClosestInstances(of: service)
                    .hop(to: eventLoop)
                    .map { instances in (service, instances) }
                    .cascade(to: lookupPromise)
                
                return lookupPromise.futureResult
                    .recover { _ in
                        // Erase all errors to an empty instance array
                        // lookup errors should only be propagated to users during calls to self.lookup(to:deadline:callback:)
                        (service, [])
                    }
                    .always {_ in timeoutTask.cancel() }
            }.cascade(to: promise)
            
            promise.futureResult.whenSuccess {
                for (service, instances) in $0 {
                    // Notify subscriptions of instance updates, if any
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
}


// MARK: Async/Await APIs
extension SixPNServiceDiscovery {
    public func findAndRegister(services: [SixPNService]) async throws {
        return try await self.findAndRegister(services: services).get()
    }
}


extension DispatchTime {
    func toNIODeadline() -> NIODeadline {
        NIODeadline.uptimeNanoseconds(self.uptimeNanoseconds)
    }
}

