/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

[<System.Obsolete("FIXME: not commutative yet")>]
module Crdt.Commutative.Pure.LSeq

open Akkling
open Crdt

type Operation<'t> =
    | Insert of index:int * value:'t
    | Remove of index:int
    
let crdt =
    { new PureCrdt<'t[], Operation<'t>> with 
        member this.Apply(state, ops) =
            ops
            |> Set.fold (fun acc op ->
                match op.Value with
                | Insert(idx, value) -> Array.insert idx value acc
                | Remove idx -> Array.removeAt idx acc) state
        member this.Default = [||]
        member this.Obsoletes(o, n) = false
    }
    
type Endpoint<'t> = Endpoint<'t[], Operation<'t>>
    
/// Used to create replication endpoint handling operation-based ORSet protocol.
let props replicaId ctx = Replicator.actor crdt replicaId ctx
/// Add new `item` into an ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins. 
let insert (index: int) (item: 'a) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Submit (Insert(index, item))
/// Remove an `item` from the ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins.
let removeAt (index: int) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Submit (Remove index)
/// Retrieve the current state of the ORSet maintained by the given `ref` endpoint. 
let query (ref: Endpoint<'a>) : Async<'a[]> = ref <? Query