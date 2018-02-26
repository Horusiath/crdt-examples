/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "gcounter.fsx"

module GCounterTests = 

    open Crdt

    let ``GCounter should inc value per node`` () =
        let (GCounter(v, _)) = 
            GCounter.zero
            |> GCounter.inc "A"     // {A:1}
            |> GCounter.inc "B"     // {A:1,B:1}
            |> GCounter.inc "A"     // {A:2,B:1}
            |> GCounter.inc "C"     // {A:2,B:1,C:1}
        v.["A"] = 2L && 
        v.["B"] = 1L && 
        v.["C"] = 1L
    
    let ``GCounter should return a cumulative value`` () =
        let value =
            GCounter.zero
            |> GCounter.inc "A"
            |> GCounter.inc "B"
            |> GCounter.inc "A"
            |> GCounter.inc "C"
            |> GCounter.value 
        value = 4L

    let ``GCounter should merge in both directions`` () =
        let g0 = GCounter.inc "A" GCounter.zero
        let gA = 
            g0
            |> GCounter.inc "A"
            |> GCounter.inc "A"            
        let gB = GCounter.inc "B" g0
        let gAB = GCounter.merge gA gB
        let gBA = GCounter.merge gB gA

        gAB = gBA && 
        GCounter.value gAB = GCounter.value gBA
        
    let ``GCounter should merge deltas`` () =
        let init = GCounter.inc "A" GCounter.zero
        let a = 
            init
            |> GCounter.inc "A"
            |> GCounter.inc "A"            
        let b = GCounter.inc "B" init
        let ab = GCounter.merge a b
        let (_, Some(d)) = GCounter.split a
        let bd = GCounter.mergeDelta b d
        
        GCounter.value ab = GCounter.value bd

open GCounterTests

printfn "[TEST] GCounter should inc value per node: %b" <| ``GCounter should inc value per node``()
printfn "[TEST] GCounter should return a cumulative value: %b" <| ``GCounter should return a cumulative value``()
printfn "[TEST] GCounter should merge in both directions: %b" <| ``GCounter should merge in both directions``()
printfn "[TEST] GCounter should merge deltass: %b" <| ``GCounter should merge deltas``()