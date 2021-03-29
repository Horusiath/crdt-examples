/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.Delta.GCounterTests

open Expecto
open Crdt.Convergent.Delta

[<Tests>]
let tests = testList "A delta-convergent GCounter" [

    test "should inc value per node" {
        let (GCounter(v, _)) = 
            GCounter.zero
            |> GCounter.inc "A"     // {A:1}
            |> GCounter.inc "B"     // {A:1,B:1}
            |> GCounter.inc "A"     // {A:2,B:1}
            |> GCounter.inc "C"     // {A:2,B:1,C:1}
            
        Expect.equal v.["A"] 2L "Partial counter of replica A should be 2" 
        Expect.equal v.["B"] 1L "Partial counter of replica B should be 1" 
        Expect.equal v.["C"] 1L "Partial counter of replica C should be 1"
    }

    test "should return a cumulative value" {
        let value =
            GCounter.zero
            |> GCounter.inc "A"
            |> GCounter.inc "B"
            |> GCounter.inc "A"
            |> GCounter.inc "C"
            |> GCounter.value 
        Expect.equal value 4L "Total counter value should be 4"
    }

    test "should merge in both directions" {
        let g0 = GCounter.inc "A" GCounter.zero
        let gA = 
            g0
            |> GCounter.inc "A"
            |> GCounter.inc "A"            
        let gB = GCounter.inc "B" g0
        let gAB = GCounter.merge gA gB
        let gBA = GCounter.merge gB gA

        Expect.equal gAB gBA "after merge both counters should be equal to each other" 
        Expect.equal (GCounter.value gAB) (GCounter.value gBA) "after merge values of both counters should be equal"
    }
        
    test "should merge deltas" {
        let init = GCounter.inc "A" GCounter.zero
        let a = 
            init
            |> GCounter.inc "A"
            |> GCounter.inc "A"            
        let b = GCounter.inc "B" init
        let ab = GCounter.merge a b
        let (_, Some(d)) = GCounter.split a
        let bd = GCounter.mergeDelta b d
        
        Expect.equal (GCounter.value ab) (GCounter.value bd) "after merging deltas both counter's values should be equal"
    }
]
