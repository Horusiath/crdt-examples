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
        let g0 = PNCounter.inc "A" PNCounter.zero
        let gA = 
            g0
            |> PNCounter.inc "A"
            |> PNCounter.dec "A"            
        let gB = PNCounter.dec "B" g0
        let gAB = PNCounter.merge gA gB
        let gBA = PNCounter.merge gB gA

        gAB = gBA && 
        PNCounter.value gAB = PNCounter.value gBA

open PNCounterTests

printfn "[TEST] PNCounter should inc and dec value per node: %b" <| ``PNCounter should inc and dec value per node``()
printfn "[TEST] PNCounter should merge in both directions: %b" <| ``PNCounter should merge in both directions``()