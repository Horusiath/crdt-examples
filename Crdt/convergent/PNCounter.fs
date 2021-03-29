/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Convergent

open Crdt

type PNCounter = PNCounter of inc:GCounter * dec:GCounter

[<RequireQualifiedAccess>]
module PNCounter =
    let zero = PNCounter(GCounter.zero, GCounter.zero)
    let value (PNCounter(inc, dec)) = GCounter.value inc - GCounter.value dec
    let inc replica value (PNCounter(inc, dec)) = PNCounter(GCounter.inc replica value inc, dec)
    let dec replica value (PNCounter(inc, dec)) = PNCounter(inc, GCounter.inc replica value dec)
    let merge (PNCounter(inc1, dec1)) (PNCounter(inc2, dec2)) = 
        PNCounter(GCounter.merge inc1 inc2, GCounter.merge dec1 dec2)
        
    [<Struct>]
    type Merge = 
        interface IConvergent<PNCounter> with
            member __.merge a b = merge a b 