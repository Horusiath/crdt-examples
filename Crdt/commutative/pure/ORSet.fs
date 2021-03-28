/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Commutative.Pure.ORSet

open Crdt
open Akkling

type Operation<'t when 't:comparison> =
    | Add of 't
    | Remove of 't

let crdt =
    { new PureCrdt<Set<'t>, Operation<'t>> with 
        member this.Apply(state, op) =
            match op with
            | Add value -> Set.add value state
            | Remove value -> Set.remove value state
        member this.Default = Set.empty
        member this.Prune(op, stable, ts) = false
        member this.Obsoletes(o, n) =
            match n.Value, o.Value with
            | Add v2, Add v1 when Version.compare n.Version o.Version = Ord.Lt -> v1 = v2    // add v2 is obsolete when its lower than another add
            | Add v2, Remove v1 when Version.compare n.Version o.Version = Ord.Lt -> v1 = v2 // add v2 is obsolete when its lower than another remove
            | Remove _, _ -> true // since o can be only lower or concurrent, it's always losing (add-wins)
            | _ -> false
    }
    
type Endpoint<'t when 't: comparison> = Endpoint<Set<'t>, Operation<'t>>
    
/// Used to create replication endpoint handling operation-based ORSet protocol.
let props db replicaId ctx = Replicator.actor crdt db replicaId ctx
/// Add new `item` into an ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins. 
let add (item: 'a) (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Submit (Add item)
/// Remove an `item` from the ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins.
let remove (item: 'a) (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Submit (Remove item)
/// Retrieve the current state of the ORSet maintained by the given `ref` endpoint. 
let query (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Query