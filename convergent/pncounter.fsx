/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "gcounter.fsx"

type PNCounter = PNCounter of inc:GCounter * dec:GCounter

[<RequireQualifiedAccess>]
module PNCounter =
    let zero = PNCounter(GCounter.zero, GCounter.zero)
    let value (PNCounter(inc, dec)) = GCounter.value inc - GCounter.value dec
    let inc replica (PNCounter(inc, dec)) = PNCounter(GCounter.inc replica inc, dec)
    let dec replica (PNCounter(inc, dec)) = PNCounter(inc, GCounter.inc replica dec)
    let merge (PNCounter(inc1, dec1)) (PNCounter(inc2, dec2)) = 
        PNCounter(GCounter.merge inc1 inc2, GCounter.merge dec1 dec2)