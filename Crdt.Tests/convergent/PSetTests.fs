module Crdt.Tests.Convergent.PSetTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent 2-Phase set" [
    test "should be able to add and remove elements and prefer removal" {
        let value = 
            PSet.zero
            |> PSet.add "a"     // expected value: [a]
            |> PSet.add "b"     // expected value: [a,b]
            |> PSet.rem "c"     // expected value: [a,b] - [c] remembered in tombstones
            |> PSet.rem "a"     // expected value: [b]   - [a,c] remembered in tombstones
            |> PSet.add "c"     // expected value: [b]   - cannot add c, once it's tombstoned
            |> PSet.add "a"     // expected value: [b]   - cannot readd a, once it's tombstoned
            |> PSet.value
        
        Expect.equal value (Set.singleton "b") "2P set doesn't show removed values"
    }

    test "should merge in both directions" {
        let init = 
            PSet.zero
            |> PSet.add "a"
            |> PSet.add "b"
        let a = 
            init
            |> PSet.add "c"
            |> PSet.rem "a"
        let b =
            init
            |> PSet.rem "c"
            |> PSet.add "d"
        let ab = PSet.merge a b
        let ba = PSet.merge b a
        
        Expect.equal ab ba "Both 2P-Sets are equal to each other after merge"
        Expect.equal (PSet.value ab) (PSet.value ba) "Values of both 2P-Sets are equal to each other after merge"
    }
]