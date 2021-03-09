module Crdt.Tests.Convergent.BCounterTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent BCounter" [
        
    test "should never be less than 0" {
        let c = BCounter.zero
        let res = BCounter.dec "A" 1L c
        
        Expect.equal res (Error 0L) "BCounter doesn't allow to decrement under total 0 value"
    }
        
    test "should not allow for higher decrement than given quota" {
        let res = 
            BCounter.zero
            |> BCounter.inc "A" 10L 
            |> BCounter.dec "A" 12L
        
        Expect.equal res (Error 10L) "BCounter doesn't allow to decrement under total 0 value"
    }

    test "should allow for decrement up to a given quota" {
        let res =
            BCounter.zero
            |> BCounter.inc "A" 10L
            |> BCounter.dec "A" 5L
            |> Result.bind (BCounter.dec "A" 5L)
            |> Result.map BCounter.value
            
        Expect.equal res (Ok 0L) "BCounter decrement is allowed within given quota bounds"
    }
        
    test "should allow for move quota to another replica" {
        let res =
            BCounter.zero
            |> BCounter.inc "A" 10L
            |> BCounter.move "A" "B" 5L
            |> Result.bind (BCounter.dec "A" 5L)
            |> Result.bind (BCounter.dec "B" 3L)
            |> Result.map BCounter.value
            
        Expect.equal res (Ok 2L) "BCounter should allow to move quota from one replica to another"
    }
        
    test "should not allow for too much quota to another replica" {
        let res =
            BCounter.zero
            |> BCounter.inc "A" 10L
            |> BCounter.move "A" "B" 11L
            
        Expect.equal res (Error 10L) "BCounter should not allow to move more quota that given replica has"
    }
]