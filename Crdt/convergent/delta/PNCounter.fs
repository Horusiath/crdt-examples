/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Convergent.Delta

open Crdt

type PNCounter = PNCounter of inc:GCounter * dec:GCounter

/// A delta-state based implementation of immutable increment/decrement counter.
[<RequireQualifiedAccess>]
module PNCounter =

    /// Creates an empty PN-Counter.
    let zero = PNCounter(GCounter.zero, GCounter.zero)
    
    /// Returns a value of provided PN-Counter.
    let value (PNCounter(inc, dec)) = GCounter.value inc - GCounter.value dec
    
    /// Increments a PN-Counter using replica `r` tag.
    let inc r (PNCounter(inc, dec)) = PNCounter(GCounter.inc r inc, dec)
    
    /// Decrements a PN-Counter using replica `r` tag.
    let dec r (PNCounter(inc, dec)) = PNCounter(inc, GCounter.inc r dec)
    
    /// Merges two PN-Counter instances together, returning another PN-Counter as results of their convergence.
    let rec merge (PNCounter(inc1, dec1)) (PNCounter(inc2, dec2)) =
        PNCounter(GCounter.merge inc1 inc2, GCounter.merge dec1 dec2)
            
    /// Merges a PN-Counter delta with PN-Counter instance, returning new G-Counter instance.
    let mergeDelta delta counter = merge counter delta
        
    /// Splits a given PN-Counter into PN-Counter with prunned delta and its delta, if provided.
    let split (PNCounter(GCounter.GCounter(inc, a), GCounter.GCounter(dec, b))) = 
        let delta =
            match a, b with
            | None, None -> None
            | _, _       -> 
                let inc = defaultArg a GCounter.zero
                let dec = defaultArg b GCounter.zero
                Some <| PNCounter(inc, dec)
        PNCounter(GCounter.GCounter(inc, None), GCounter.GCounter(dec, None)), delta
