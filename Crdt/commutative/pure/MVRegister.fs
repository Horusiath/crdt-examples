/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Commutative.Pure.MVRegister

open Crdt
open Akkling

let crdt =
    { new PureCrdt<'t list, 't> with
        member this.Default = []
        member this.Obsoletes(o, n): bool = Version.compare n.Version o.Version <= Ord.Eq 
        member this.Apply(state, ops) =
            ops
            |> Seq.map (fun o -> o.Value)
            |> Seq.toList }

type Endpoint<'t> = Endpoint<'t list, 't>
    
let props replica ctx = Replicator.actor crdt replica ctx
let update (value: 'a) (ref: Endpoint<'a>) : Async<'a list> = ref <? Submit value
let query (ref: Endpoint<'a>) : Async<'a list> = ref <? Query