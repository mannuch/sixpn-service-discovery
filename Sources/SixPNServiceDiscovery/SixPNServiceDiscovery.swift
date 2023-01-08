import Atomics
import Dispatch
import ServiceDiscovery

public final class SixPNServiceDiscovery<Service: Hashable, Instance: Hashable>: ServiceDiscovery {
    public func subscribe(to service: Service, onNext nextResultHandler: @escaping (Result<[Instance], Error>) -> Void, onComplete completionHandler: @escaping (CompletionReason) -> Void) -> CancellationToken {
        
    }
    
    public func lookup(_ service: Service, deadline: DispatchTime?, callback: @escaping (Result<[Instance], Error>) -> Void) {
        
    }
    
    
    public var defaultLookupTimeout: DispatchTimeInterval
    

    public init() {}
}
