/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative

open Crdt
open Akkling

[<RequireQualifiedAccess>]    
module ORMap =
    
    type ORMap<'k, 'v when 'k: comparison> = Map<'k, (VTime * 'v) list>
    
    type Command<'k, 'v when 'k: comparison> =
        | Update of key:'k * value:'v
        | Remove of key:'k
        
    type Operation<'k,'v when 'k: comparison> =
        | Updated of key:'k * value:'v
        | Removed of key:'k * Set<VTime>
            
    type Endpoint<'k,'v> when 'k: comparison = Endpoint<ORMap<'k,'v>, Command<'k,'v>, Operation<'k,'v>>
    
    let private crdt : Crdt<ORMap<'k, 'v>, Map<'k, 'v list>, Command<'k,'v>, Operation<'k,'v>> =
        { new Crdt<_,_,_,_> with
            member _.Default = Map.empty
            member _.Query(ormap) = ormap |> Map.map (fun k v -> List.map snd v)  
            member _.Prepare(ormap, cmd) =
                match cmd with
                | Update(key,value) -> Updated(key, value)
                | Remove key ->
                    match Map.tryFind key ormap with
                    | None -> Removed(key, Set.empty)
                    | Some values ->
                        let timestamps = values |> Seq.map fst |> Set.ofSeq
                        Removed(key, timestamps)
            member _.Effect(ormap, e) =
                match e.Data with
                | Updated(key, value) ->
                    let entry = (e.Version, value)
                    match Map.tryFind key ormap with
                    | None -> Map.add key [entry] ormap
                    | Some existing ->
                        let concurrent =
                            existing
                            |> List.filter (fun (vt, _) -> Version.compare vt e.Version = Ord.Cc)                        
                        Map.add key (entry::concurrent) ormap
                | Removed(key, timestamps) ->
                    match Map.tryFind key ormap with
                    | None -> ormap
                    | Some values ->
                        let remaining =
                            values
                            |> List.filter (fun (ts, _) -> not (Set.contains ts timestamps))
                        if List.isEmpty remaining then Map.remove key ormap
                        else Map.add key remaining ormap
        }
    
    /// Used to create replication endpoint handling operation-based ORSet protocol.
    let props db replicaId ctx = replicator crdt db replicaId ctx
    
    /// Add new `item` into an ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins. 
    let update (key: 'k) (value: 'v) (ref: Endpoint<'k,'v>) : Async<Map<'k, 'v list>> = ref <? Command (Update(key, value))
    
    /// Remove an `item` from the ORSet maintained by the given `ref` endpoint. In case of add/remove conflicts add wins.
    let remove (key: 'k) (ref: Endpoint<'k,'v>) : Async<Map<'k,'v list>> = ref <? Command (Remove key)
    
    /// Retrieve the current state of the ORSet maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'k,'v>) : Async<Map<'k, 'v list>> = ref <? Query
    