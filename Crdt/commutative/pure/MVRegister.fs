/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Commutative.Pure.MVRegister

open Crdt
open Akkling

let crdt =
    { new PureCrdt<'t list, 't> with 
        member this.Apply(state, op) = op::state
        member this.Default = []
        member this.Prune(op, stable, ts) = false
        member this.Obsoletes(o, n): bool = Version.compare n.Version o.Version <= Ord.Eq }

type Endpoint<'t> = Endpoint<'t list, 't>
    
let props db replica ctx = Replicator.actor crdt db replica ctx
let update (value: 'a) (ref: Endpoint<'a>) : Async<'a list> = ref <? Submit value
let query (ref: Endpoint<'a>) : Async<'a list> = ref <? Query