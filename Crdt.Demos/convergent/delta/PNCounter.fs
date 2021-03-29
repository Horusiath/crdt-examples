/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.DeltaPNCounter

open Crdt
open Crdt.Convergent.Delta

let ``PNCounter should inc and dec value per node`` () =
    let value = 
        PNCounter.zero
        |> PNCounter.inc "A"
        |> PNCounter.inc "B"
        |> PNCounter.inc "A"
        |> PNCounter.dec "C"
        |> PNCounter.inc "C"
        |> PNCounter.dec "A"
        |> PNCounter.value
    value = 2L

let ``PNCounter should merge in both directions`` () =
    let init = PNCounter.inc "A" PNCounter.zero
    let a = 
        init
        |> PNCounter.inc "A"
        |> PNCounter.dec "A"            
    let b = PNCounter.dec "B" init
    let ab = PNCounter.merge a b
    let ba = PNCounter.merge b a

    PNCounter.value a = PNCounter.value ba
    
let ``PNCounter should merge deltas`` () =
    let init = PNCounter.inc "A" PNCounter.zero
    let a = 
        init
        |> PNCounter.inc "A"
        |> PNCounter.dec "A"            
    let b = PNCounter.dec "B" init
    let ab = PNCounter.merge a b
    let (_, Some(d)) = PNCounter.split a
    let bd = PNCounter.mergeDelta b d

    PNCounter.value ab = PNCounter.value bd