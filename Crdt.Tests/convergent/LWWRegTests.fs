/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.LWWRegTests

open System
open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent Last-Write-Wins Register" [
        
    test "should be able to set value with clock attached" {
        let value= 
            LWWReg.zero()
            |> LWWReg.set DateTime.UtcNow 1 
            |> LWWReg.value
            
        Expect.equal value 1 "LWW register value should be set"
    }

    test "should always pick the latest value" {
        let now = DateTime.UtcNow
        let sec = TimeSpan.FromSeconds 1.
        let reg =
            LWWReg.zero()
            |> LWWReg.set now 1
            |> LWWReg.set (now + sec) 2
            |> LWWReg.set (now - sec) 3

        Expect.equal (LWWReg.value reg) 2 "LWW register should return the latest value"
        Expect.equal reg (LWWReg(2, now + sec)) "LWW register should point to the latest timestamp"
    }
]