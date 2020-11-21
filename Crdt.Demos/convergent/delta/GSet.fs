/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Demos.DeltaGSet

open Crdt
open Crdt.Convergent.Delta

let ``GSet should add elements`` () =
    let value =
        GSet.zero
        |> GSet.add "a"
        |> GSet.add "c"
        |> GSet.add "b"
        |> GSet.add "c"
        |> GSet.value
    value = Set.ofList [ "a"; "b"; "c" ]

let ``GSet should merge both ways`` () =
    let init = GSet.add "x" GSet.zero
    let a = 
        init
        |> GSet.add "y"
        |> GSet.add "z"
    let b = 
        init
        |> GSet.add "u"
        |> GSet.add "y"
    let ab = GSet.merge a b
    let ba = GSet.merge b a
    ab = ba &&
    GSet.value ab = GSet.value ba

let ``Get should merge deltas`` () =
    let init = GSet.add "x" GSet.zero
    let a = 
        init
        |> GSet.add "y"
        |> GSet.add "z"
    let b = 
        init
        |> GSet.add "u"
        |> GSet.add "y"
    let ab = GSet.merge a b
    let (_, Some(d)) = GSet.split a
    let bd = GSet.mergeDelta b d

    GSet.value ab = GSet.value bd