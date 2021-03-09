module Crdt.Tests.Convergent.Delta.MVRegTests

open Expecto
open Crdt.Convergent.Delta

[<Tests>]
let tests = testList "A delta-convergent Multi-Value Register" [
        
    test "should allow to override values for their causal past" {
        let register =
            MVReg.zero
            |> MVReg.set "A" 1
            |> MVReg.set "A" 2
            |> MVReg.set "A" 3
        
        Expect.equal (MVReg.value register) [3] "mv-register should return the latest value"
    }
        
    test "should preserve multiple values updated concurrently" {
        let init = 
            MVReg.zero
            |> MVReg.set "A" 1
        let b = init |> MVReg.set "B" 2
        let a = init |> MVReg.set "A" 3
        let c = MVReg.merge a b
        
        Expect.equal (MVReg.value c) [3;2] "mv-register should return values of both concurrent updates"
    }
        
    test "should go back to setting values in a single timeline after merge" {
        let init = 
            MVReg.zero
            |> MVReg.set "A" 1
        let b = init |> MVReg.set "B" 2
        let a = init |> MVReg.set "A" 3
        let c = MVReg.merge a b
        let d = c |> MVReg.set "A" 4
        
        Expect.equal (MVReg.value d) [4] "mv-register should allow to override concurrent conflicts"
    }
        
    test "should merge in both directions" {
        let init =
            MVReg.zero
            |> MVReg.set "A" 1
        let a = 
            init
            |> MVReg.set "A" 2
        let b =
            init 
            |> MVReg.set "B" 3
        let ab = MVReg.merge a b
        let ba = MVReg.merge b a
        
        Expect.equal ab ba "both registers should be equal after merge"
        Expect.equal (MVReg.value ab) (MVReg.value ba) "both register's values should be equal after merge"
    }
]
