/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.DeltaAWORSet

open Crdt
open Crdt.Convergent.Delta

let ``AWORSet should add, remove and re-add elements in the same replica`` () = 
    let value =
        AWORSet.zero
        |> AWORSet.add "A" 1        // result: [1]
        |> AWORSet.add "A" 2        // result: [1,2]
        |> AWORSet.rem "A" 1        // result: [2]
        |> AWORSet.add "A" 3        // result: [2,3]
        |> AWORSet.add "A" 1        // result: [1,2,3]
        |> AWORSet.rem "A" 2        // result: [1,3]
        |> AWORSet.value
    value = Set.ofList [1;3]

let ``AWORSet should prefer adds over rems in concurrent updates`` () = 
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
    AWORSet.value value = Set.ofList [1;3]

let ``AWORSet should merge in both directions`` () =
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
    ab = ba &&
    AWORSet.value ab = AWORSet.value ba