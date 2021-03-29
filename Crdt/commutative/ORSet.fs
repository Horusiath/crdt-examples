/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Commutative

open Crdt
open Akkling

[<RequireQualifiedAccess>]    
module ORSet =
    
    type ORSet<'a> when 'a: comparison = Set<'a * VTime>
    
    type Command<'a> =
        | Add of 'a
        | Remove of 'a
    
    type Operation<'a> =
        | Added of 'a
        | Removed of Set<VTime>
    
    type Endpoint<'a> when 'a: comparison = Endpoint<ORSet<'a>, Command<'a>, Operation<'a>>
    
    let private crdt : Crdt<ORSet<'a>, Set<'a>, Command<'a>, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default = Set.empty
            member _.Query(orset) = orset |> Set.map fst   
            member _.Prepare(orset, cmd) =
                match cmd with
                | Add item -> Added(item)
                | Remove item ->
                    let timestamps =
                        orset
                        |> Set.filter (fun (i, _) -> i = item)
                        |> Set.map snd
                    Removed timestamps
            member _.Effect(orset, e) =
                match e.Data with
                | Added item -> Set.add (item, e.Version) orset
                | Removed versions -> orset |> Set.filter (fun (_, ts) -> not (Set.contains ts versions)) }
    
    /// Used to create replication endpoint handling operation-based ORSet protocol.
    let props db replicaId ctx = replicator crdt db replicaId ctx
    
    /// Add new `item` into an ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins. 
    let add (item: 'a) (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Command (Add item)
    
    /// Remove an `item` from the ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins.
    let remove (item: 'a) (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Command (Remove item)
    
    /// Retrieve the current state of the ORSet maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'a>) : Async<Set<'a>> = ref <? Query