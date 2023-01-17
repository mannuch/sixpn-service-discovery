//
//  DNS.swift
//  
//
//  Created by Matthew Mannucci on 1/16/23.
//

import DNSClient
import NIOCore
import Logging
import XCTestDynamicOverlay

public protocol SixPNDNSClient {
    func getAllAppNames() -> EventLoopFuture<[String]>
    func getTopNClosestInstances(of service: SixPNService) -> EventLoopFuture<[SocketAddress]>
    func close() -> Void
}

public final class LiveDNSClient: SixPNDNSClient {
    private let eventLoop: EventLoop
    private let dnsClient: DNSClient
    private let logger: Logger
    
    private init(
        on eventLoop: EventLoop,
        dnsClient: DNSClient,
        logger: Logger = .init(label: "swift-service-discovery.sixpn.dns-client")
    ) {
        self.eventLoop = eventLoop
        self.dnsClient = dnsClient
        self.logger = logger
    }
    
    public static func connect(on eventLoop: EventLoop, logger: Logger) -> EventLoopFuture<LiveDNSClient> {
        DNSClient.connect(on: eventLoop).map { dnsClient in
                .init(on: eventLoop, dnsClient: dnsClient, logger: logger)
        }
    }
    
    public func getAllAppNames() -> EventLoopFuture<[String]> {
        let promise = self.eventLoop.makePromise(of: [String].self)
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
            promise.succeed(appNames)
        }
        return promise.futureResult
    }
    
    public func getTopNClosestInstances(of service: SixPNService) -> EventLoopFuture<[SocketAddress]> {
        let promise = self.eventLoop.makePromise(of: [SocketAddress].self)
        
        self.dnsClient.initiateAAAAQuery(
            host: "top\(service.topNClosestInstances).nearest.of.\(service.appName).internal",
            port: Int(service.port)
        )
        .hop(to: self.eventLoop)
        .whenComplete { result in
            switch result {
            case let .success(socketAddresses):
                self.logger.info("Found \(socketAddresses.count) instances of service ['\(service.appName)', port: \(service.port)]")
                promise.succeed(socketAddresses)
            case let .failure(error):
                self.logger.warning("Error finding instances of service ['\(service.appName)', port: \(service.port)]: '\(error)'")
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    public func close() {
        self.dnsClient.cancelQueries()
    }
    
}

public final class TestDNSClient: SixPNDNSClient {
    public struct Implementation {
        public var allSixPNApps: () -> EventLoopFuture<[String]>
        public var topNClosestInstances: (SixPNService) -> EventLoopFuture<[SocketAddress]>
        public var close: () -> Void
        public init(
            allSixPNApps: @escaping () -> EventLoopFuture<[String]>,
            topNClosestInstances: @escaping (SixPNService) -> EventLoopFuture<[SocketAddress]>,
            close: @escaping () -> Void
        ) {
            self.allSixPNApps = allSixPNApps
            self.topNClosestInstances = topNClosestInstances
            self.close = close
        }
    }
    
    public var implementation: Implementation
    
    public init(_ implementation: Implementation) {
        self.implementation = implementation
    }
    
    public func getAllAppNames() -> EventLoopFuture<[String]> {
        implementation.allSixPNApps()
    }
    
    public func getTopNClosestInstances(of service: SixPNService) -> EventLoopFuture<[SocketAddress]> {
        implementation.topNClosestInstances(service)
    }
    
    public func close() {
        implementation.close()
    }
}

extension TestDNSClient.Implementation {
    static func unimplemented(on eventLoop: EventLoop) -> Self {
        .init(
            allSixPNApps: XCTestDynamicOverlay.unimplemented("\(Self.self).allSixPNApps", placeholder: eventLoop.makeSucceededFuture([])),
            topNClosestInstances: XCTestDynamicOverlay.unimplemented("\(Self.self).topNClosestInstances", placeholder: eventLoop.makeSucceededFuture([])),
            close: XCTestDynamicOverlay.unimplemented("\(Self.self).close")
        )
    }
}

extension TestDNSClient {
    public static func unimplemented(on eventLoop: EventLoop) -> Self {
        return .init(.unimplemented(on: eventLoop))
    }
}
