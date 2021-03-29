/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.ConvergentAWORMap

open Crdt
open Crdt.Convergent

[<Struct>]
type TestMerge =
    interface IConvergent<int> with
        member __.merge a b = max a b

let ``AWORMap should add, remove and readd elements`` () = 
    let actual =
        AWORMap.zero<string,int,TestMerge>()
        |> AWORMap.add "A" "key1" 1
        |> AWORMap.add "A" "key2" 2
        |> AWORMap.add "A" "key3" 3
        |> AWORMap.rem "A" "key1"
        |> AWORMap.add "A" "key1" 4
        |> AWORMap.rem "A" "key3"
        |> AWORMap.add "A" "key2" 5
        |> AWORMap.value
    let expected = Map.ofList [
        "key1", 4
        "key2", 5
    ]
    actual = expected

let ``AWORMap should prefer add before merge in concurrent updates`` () =
    let init =
        AWORMap.zero<string,int,TestMerge>()
        |> AWORMap.add "A" "key1" 1
        |> AWORMap.add "A" "key2" 2
        |> AWORMap.add "A" "key3" 3 // {key1:1, key2:2, key3:3}
    let a =
        init
        |> AWORMap.add "A" "key4" 4
        |> AWORMap.add "A" "key1" 5
        |> AWORMap.rem "A" "key3"   // {key1:5, key2:2, key4:4}
    let b =
        init
        |> AWORMap.add "A" "key1" 2
        |> AWORMap.add "A" "key3" 6
        |> AWORMap.rem "A" "key2"   // {key1:2, key3:6}
    let actual =
        AWORMap.merge a b
        |> AWORMap.value            // {key1:5, key3:6, key4:4}
    printfn "%A" actual
    let expected = Map.ofList [
        "key1", 5
        "key3", 6
        "key4", 4
    ]
    actual = expected

let ``AWORMap should merge in both directions`` () =
    let init =
        AWORMap.zero<string,int,TestMerge>()
        |> AWORMap.add "A" "key1" 1
        |> AWORMap.add "B" "key2" 2
    let a = 
        init
        |> AWORMap.rem "A" "key1"
        |> AWORMap.rem "A" "key2"
    let b =
        init 
        |> AWORMap.add "B" "key1" 1
        |> AWORMap.add "B" "key3" 3
    let ab = AWORMap.merge a b
    let ba = AWORMap.merge b a
    ab = ba &&
    AWORMap.value ab = AWORMap.value ba