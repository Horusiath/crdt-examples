/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "gcounter.fsx"

module GCounterTests = 

    open Crdt

    let ``GCounter should inc value per node`` () =
        let (GCounter(v)) = 
            GCounter.zero
            |> GCounter.inc "A" 1L     // {A:1}
            |> GCounter.inc "B" 1L     // {A:1,B:1}
            |> GCounter.inc "A" 1L     // {A:2,B:1}
            |> GCounter.inc "C" 1L     // {A:2,B:1,C:1}
        v.["A"] = 2L && 
        v.["B"] = 1L && 
        v.["C"] = 1L
    
    let ``GCounter should return a cumulative value`` () =
        let value =
            GCounter.zero
            |> GCounter.inc "A" 1L
            |> GCounter.inc "B" 1L
            |> GCounter.inc "A" 1L
            |> GCounter.inc "C" 1L
            |> GCounter.value 
        value = 4L

    let ``GCounter should merge in both directions`` () =
        let g0 = GCounter.inc "A" 1L GCounter.zero
        let gA = 
            g0
            |> GCounter.inc "A" 1L
            |> GCounter.inc "A" 1L         
        let gB = GCounter.inc "B" 3L g0
        let gAB = GCounter.merge gA gB
        let gBA = GCounter.merge gB gA

        gAB = gBA && 
        GCounter.value gAB = GCounter.value gBA

open GCounterTests

printfn "[TEST] GCounter should inc value per node: %b" <| ``GCounter should inc value per node``()
printfn "[TEST] GCounter should return a cumulative value: %b" <| ``GCounter should return a cumulative value``()
printfn "[TEST] GCounter should merge in both directions: %b" <| ``GCounter should merge in both directions``()