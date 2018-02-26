/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "pncounter.fsx"

module PNCounterTests =

    open Crdt

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


open PNCounterTests

printfn "[TEST] PNCounter should inc and dec value per node: %b" <| ``PNCounter should inc and dec value per node``()
printfn "[TEST] PNCounter should merge in both directions: %b" <| ``PNCounter should merge in both directions``()
printfn "[TEST] PNCounter should merge deltas: %b" <| ``PNCounter should merge deltas``()