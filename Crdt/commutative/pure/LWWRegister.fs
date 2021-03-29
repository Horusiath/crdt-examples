/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

[<RequireQualifiedAccess>]
module Crdt.Commutative.Pure.LWWRegister

open Crdt
open Akkling

let private comparer (o: Op<_>) = struct(o.Timestamp, o.Origin) 
let crdt =
    { new PureCrdt<'t voption, 't> with 
        member this.Default = ValueNone
        member this.Obsoletes(o, n) = comparer o > comparer n
        member this.Apply(state, ops) =
            let latest = ops |> Seq.maxBy comparer
            ValueSome latest.Value }

type Endpoint<'t> = Endpoint<'t voption, 't>

let props replica ctx = Replicator.actor crdt replica ctx
let update (value: 'a) (ref: Endpoint<'a>) : Async<'a voption> = ref <? Submit value
let query (ref: Endpoint<'a>) : Async<'a voption> = ref <? Query