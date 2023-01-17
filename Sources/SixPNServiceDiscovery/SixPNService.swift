//
//  SixPNService.swift
//  
//
//  Created by Matthew Mannucci on 1/16/23.
//

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
