/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.DeltaMVReg

open Crdt
open Crdt.Convergent.Delta

let ``MVReg should allow to override values for their causal past`` () =
    MVReg.zero
    |> MVReg.set "A" 1
    |> MVReg.set "A" 2
    |> MVReg.set "A" 3
    |> MVReg.value = [3]
    
let ``MVReg should preserve mutliple values updated concurrently`` () =
    let init = 
        MVReg.zero
        |> MVReg.set "A" 1
    let b = init |> MVReg.set "B" 2
    let a = init |> MVReg.set "A" 3
    MVReg.merge a b
    |> MVReg.value = [2;3]
    
let ``MVReg should go back to setting values in a single timeline after merge`` () =
    let init = 
        MVReg.zero
        |> MVReg.set "A" 1
    let b = init |> MVReg.set "B" 2
    let a = init |> MVReg.set "A" 3
    MVReg.merge a b
    |> MVReg.set "A" 4
    |> MVReg.value = [4]
    
let ``MVReg should merge in both directions`` () =
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
    ab = ba &&
    MVReg.value ab = MVReg.value ba