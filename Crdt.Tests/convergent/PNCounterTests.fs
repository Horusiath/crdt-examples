/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.PNCounterTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent PNCounter" [

    test "should inc and dec value per node" {
        let value = 
            PNCounter.zero
            |> PNCounter.inc "A" 1L
            |> PNCounter.inc "B" 1L
            |> PNCounter.inc "A" 1L
            |> PNCounter.dec "C" 1L
            |> PNCounter.inc "C" 1L
            |> PNCounter.dec "A" 1L
            |> PNCounter.value
        
        Expect.equal value 2L "PNCounter total value should include increments and decrements"
    }

    test "should merge in both directions" {
        let g0 = PNCounter.inc "A" 1L PNCounter.zero
        let gA = 
            g0
            |> PNCounter.inc "A" 1L
            |> PNCounter.dec "A" 2L         
        let gB = PNCounter.dec "B" 1L g0
        let gAB = PNCounter.merge gA gB
        let gBA = PNCounter.merge gB gA

        Expect.equal gAB gBA "Both PNCounters should be equal to each other after merge"
        Expect.equal (PNCounter.value gAB) (PNCounter.value gBA) "Values of both counters should be equal to each other after merge"
    }
]