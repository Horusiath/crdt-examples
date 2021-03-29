/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.ConvergentPNCounter

open Crdt
open Crdt.Convergent

let ``PNCounter should inc and dec value per node`` () =
    let value = 
        PNCounter.zero
        |> PNCounter.inc "A" 1L
        |> PNCounter.inc "B" 1L
        |> PNCounter.inc "A" 1L
        |> PNCounter.dec "C" 1L
        |> PNCounter.inc "C" 1L
        |> PNCounter.dec "A" 1L
        |> PNCounter.value
    value = 2L

let ``PNCounter should merge in both directions`` () =
    let g0 = PNCounter.inc "A" 1L PNCounter.zero
    let gA = 
        g0
        |> PNCounter.inc "A" 1L
        |> PNCounter.dec "A" 2L         
    let gB = PNCounter.dec "B" 1L g0
    let gAB = PNCounter.merge gA gB
    let gBA = PNCounter.merge gB gA

    gAB = gBA && 
    PNCounter.value gAB = PNCounter.value gBA