/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.AWORSetTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent Add-Wins Observed Remove Set" [
    test "should add, remove and re-add elements in the same replica" {
        let value =
            AWORSet.zero
            |> AWORSet.add "A" 1        // result: [1]
            |> AWORSet.add "A" 2        // result: [1,2]
            |> AWORSet.rem "A" 1        // result: [2]
            |> AWORSet.add "A" 3        // result: [2,3]
            |> AWORSet.add "A" 1        // result: [1,2,3]
            |> AWORSet.rem "A" 2        // result: [1,3]
            |> AWORSet.value
            
        Expect.equal value (Set.ofList [1;3]) "elements added, removed then added again should appear in the result set"
    }

    test "should prefer adds over rems in concurrent updates" {
        let init =
            AWORSet.zero
            |> AWORSet.add "A" 1
            |> AWORSet.add "B" 2
            
        let a = 
            init
            |> AWORSet.rem "A" 1
            |> AWORSet.rem "A" 2
            
        let b =
            init 
            |> AWORSet.add "B" 1    // concurrent update with A:rem(1) - preferred add
            |> AWORSet.add "B" 3
            
        let value = 
            AWORSet.merge a b
            |> AWORSet.value
            
        Expect.equal value (Set.ofList [1;3]) "after merge concurrent conflict resolution should be add-wins"
    }

    test "should merge in both directions" {
        let init =
            AWORSet.zero
            |> AWORSet.add "A" 1
            |> AWORSet.add "B" 2
            
        let a = 
            init
            |> AWORSet.rem "A" 1
            |> AWORSet.rem "A" 2
            
        let b =
            init 
            |> AWORSet.add "B" 1    // concurrent update with A:rem(1) - preferred add
            |> AWORSet.add "B" 3
            
        let ab = AWORSet.merge a b
        let ba = AWORSet.merge b a
        
        Expect.equal ab ba "both OR-Sets should be equal to each other after merge"
        Expect.equal (AWORSet.value ab) (AWORSet.value ba) "values of both sets should be equal after merge"
    }
]