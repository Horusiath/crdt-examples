/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

[<RequireQualifiedAccess>]
module Crdt.Commutative.Pure.Counter

open Akkling

let crdt =     
    { new PureCrdt<int64, int64> with
       member this.Default = 0L
       member this.Obsoletes(o, n) = false
       member this.Apply(state, ops) =
           ops
           |> Seq.map (fun o -> o.Value)
           |> Seq.fold (+) state }

/// Used to create replication endpoint handling operation-based Counter protocol.
let props replica ctx = Replicator.actor crdt replica ctx

/// Increment counter maintainer by given `ref` endpoint by a given delta (can be negative).
let inc (by: int64) (ref: Endpoint<int64,int64>) : Async<int64> = ref <? Submit by

/// Retrieve the current state of the counter maintained by the given `ref` endpoint. 
let query (ref: Endpoint<int64,int64>) : Async<int64> = ref <? Query