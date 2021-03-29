/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.GCounterTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent GCounter" [
        
    test "should inc value per node" {
        let (GCounter(v)) = 
            GCounter.zero
            |> GCounter.inc "A" 1L     // {A:1}
            |> GCounter.inc "B" 1L     // {A:1,B:1}
            |> GCounter.inc "A" 1L     // {A:2,B:1}
            |> GCounter.inc "C" 1L     // {A:2,B:1,C:1}
            
        Expect.equal v.["A"] 2L "A partial counter should be 2" 
        Expect.equal v.["B"] 1L "B partial counter should be 1" 
        Expect.equal v.["C"] 1L "C partial counter should be 1"
    }

    test "should return a cumulative value" {
        let value =
            GCounter.zero
            |> GCounter.inc "A" 1L
            |> GCounter.inc "B" 1L
            |> GCounter.inc "A" 1L
            |> GCounter.inc "C" 1L
            |> GCounter.value
            
        Expect.equal value 4L "Total counter value should be 4"
    }

    test "should merge in both directions" {
        let g0 = GCounter.inc "A" 1L GCounter.zero
        
        let gA = 
            g0
            |> GCounter.inc "A" 1L
            |> GCounter.inc "A" 1L
            
        let gB = GCounter.inc "B" 3L g0
        
        let gAB = GCounter.merge gA gB
        let gBA = GCounter.merge gB gA

        Expect.equal gAB gBA "Both counter should be equal after merge" 
        Expect.equal (GCounter.value gAB) (GCounter.value gBA) "Values of both counters should be equal after merge"
    }
]