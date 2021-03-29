/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.ConvergentLWWReg

open System
open Crdt
open Crdt.Convergent

let ``Last-Write-Wins register should be able to set value with clock attached`` () =
    let value= 
        LWWReg.zero()
        |> LWWReg.set DateTime.UtcNow 1 
        |> LWWReg.value
    value = 1

let ``Last-Write-Wins register should always pick the latest value`` () =
    let now = DateTime.UtcNow
    let sec = TimeSpan.FromSeconds 1.
    let reg =
        LWWReg.zero()
        |> LWWReg.set now 1
        |> LWWReg.set (now + sec) 2
        |> LWWReg.set (now - sec) 3

    LWWReg.value reg = 2 &&
    reg = LWWReg(2, now + sec)