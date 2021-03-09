module Crdt.Tests.Convergent.Delta.AWORSetTests

open Expecto
open Crdt.Convergent.Delta

[<Tests>]
let tests = testList "A delta-convergent Add-Wins Observed Remove Set" [
            
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
            
        Expect.equal value (Set.ofList [1;3]) "returns a set with value that was added, removed then added again"
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
        
        Expect.equal (AWORSet.value value) (Set.ofList [1;3]) "1 should be present in result set (add-wins in concurrent update)"
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
        
        Expect.equal ab ba "after merge both sets are equal"
        Expect.equal (AWORSet.value ab) (AWORSet.value ba) "after merge values of both sets are equal"
    }
]