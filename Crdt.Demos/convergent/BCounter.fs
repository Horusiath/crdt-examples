/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.ConvergentBCounter

open Crdt
open Crdt.Convergent

let ``BCounter should never be less than 0`` () =
    let c = BCounter.zero
    let res = BCounter.dec "A" 1L c
    res = Error 0L
    
let ``BCounter should not allow for higher decrement than given quota`` () =
    let res = 
        BCounter.zero
        |> BCounter.inc "A" 10L 
        |> BCounter.dec "A" 12L
    res = Error 10L

let ``BCounter should allow for decrement up to a given quota`` () =
    let res =
        BCounter.zero
        |> BCounter.inc "A" 10L
        |> BCounter.dec "A" 5L
        |> Result.bind (BCounter.dec "A" 5L)
        |> Result.map BCounter.value
    res = Ok 0L
    
let ``BCounter should allow for move quota to another replica`` () =
    let res =
        BCounter.zero
        |> BCounter.inc "A" 10L
        |> BCounter.move "A" "B" 5L
        |> Result.bind (BCounter.dec "A" 5L)
        |> Result.bind (BCounter.dec "B" 3L)
        |> Result.map BCounter.value
    res = Ok 2L
    
let ``BCounter should not allow for too much quota to another replica`` () =
    let res =
        BCounter.zero
        |> BCounter.inc "A" 10L
        |> BCounter.move "A" "B" 11L
    res = Error 10L