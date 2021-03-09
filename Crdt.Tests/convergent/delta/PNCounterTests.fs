module Crdt.Tests.Convergent.Delta.PNCounterTests

open Expecto
open Crdt.Convergent.Delta

[<Tests>]
let tests = testList "A delta-convergent PNCounter" [

    test "should inc and dec value per node" {
        let value = 
            PNCounter.zero
            |> PNCounter.inc "A"
            |> PNCounter.inc "B"
            |> PNCounter.inc "A"
            |> PNCounter.dec "C"
            |> PNCounter.inc "C"
            |> PNCounter.dec "A"
            |> PNCounter.value
            
        Expect.equal value 2L "total PNCounter value should be 2"
    }

    test "should merge in both directions" {
        let init = PNCounter.inc "A" PNCounter.zero
        let a = 
            init
            |> PNCounter.inc "A"
            |> PNCounter.dec "A"            
        let b = PNCounter.dec "B" init
        let ab = PNCounter.merge a b
        let ba = PNCounter.merge b a

        Expect.equal ab ba "after merge both counters should be equal to each other"
        Expect.equal (PNCounter.value ab) (PNCounter.value ba) "after merge both counters should have the same value"
    }
        
    test "should merge deltas" {
        let init = PNCounter.inc "A" PNCounter.zero
        let a = 
            init
            |> PNCounter.inc "A"
            |> PNCounter.dec "A"            
        let b = PNCounter.dec "B" init
        let ab = PNCounter.merge a b
        let (_, Some(d)) = PNCounter.split a
        let bd = PNCounter.mergeDelta b d

        Expect.equal (PNCounter.value ab) (PNCounter.value bd) "after merging deltas, both counter should have the same value"
    }
]
