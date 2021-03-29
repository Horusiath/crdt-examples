/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.Delta.GSetTests

open Expecto
open Crdt.Convergent.Delta

[<Tests>]
let tests = testList "A delta-convergent Grow-only Set" [
        
    test "should add elements" {
        let value =
            GSet.zero
            |> GSet.add "a"
            |> GSet.add "c"
            |> GSet.add "b"
            |> GSet.add "c"
            |> GSet.value
            
        Expect.equal value (Set.ofList [ "a"; "b"; "c" ]) "should return deduplicated values"
    }

    test "should merge both ways" {
        let init = GSet.add "x" GSet.zero
        let a = 
            init
            |> GSet.add "y"
            |> GSet.add "z"
        let b = 
            init
            |> GSet.add "u"
            |> GSet.add "y"
        let ab = GSet.merge a b
        let ba = GSet.merge b a
        
        Expect.equal ab ba "after merge, both sets should be equal to each other"
        Expect.equal (GSet.value ab) (GSet.value ba) "after merge, both sets should return equal values"
    }
        
    test "should merge deltas" {
        let init = GSet.add "x" GSet.zero
        let a = 
            init
            |> GSet.add "y"
            |> GSet.add "z"
        let b = 
            init
            |> GSet.add "u"
            |> GSet.add "y"
        let ab = GSet.merge a b
        let (_, Some(d)) = GSet.split a
        let bd = GSet.mergeDelta b d

        Expect.equal (GSet.value ab) (GSet.value bd) "after delta-merge both sets should have equal values"
    }
]