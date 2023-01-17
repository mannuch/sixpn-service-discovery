//
//  SixPNError.swift
//  
//
//  Created by Matthew Mannucci on 1/16/23.
//

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
