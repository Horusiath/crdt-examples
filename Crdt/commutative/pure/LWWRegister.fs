/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

[<RequireQualifiedAccess>]
module Crdt.Commutative.Pure.LWWRegister

open Crdt
open Akkling

let private redundant (o: Versioned<_>) (n: Versioned<_>) =
    let cmp = n.Timestamp.CompareTo o.Timestamp
    if cmp = 0 then n.Origin < o.Origin
    else cmp < 0

let crdt =
    { new PureCrdt<'t voption, 't> with 
        member this.Apply(state, op) =
            //printfn "LWW: %O => %O" state op
            ValueSome op
        member this.Default = ValueNone
        member this.Prune(op, stable, ts) = false
        member this.Obsoletes(o, n) = redundant o n }

type Endpoint<'t> = Endpoint<'t voption, 't>

let props db replica ctx = Replicator.actor crdt db replica ctx
let update (value: 'a) (ref: Endpoint<'a>) : Async<'a voption> = ref <? Submit value
let query (ref: Endpoint<'a>) : Async<'a voption> = ref <? Query