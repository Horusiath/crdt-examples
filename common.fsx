/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt 

open System

type ReplicaId = String
type Ord = 
    | Lt = -1  // lower
    | Eq = 0   // equal
    | Gt = 1   // greater
    | Cc = 2   // concurrent

[<RequireQualifiedAccess>]
module Helpers =

    /// Helper method for insert-or-update semantic for Map
    let upsert k v fn map =
        match Map.tryFind k map with
        | None -> Map.add k v map
        | Some v -> Map.add k (fn v) map

    let tup2map (k, v) = Map.add k v Map.empty

    /// General merge used on values wrapped with options
    let mergeOption merge a b =
        match a, b with
        | Some x, Some y -> Some (merge x y)
        | Some x, None   -> Some x
        | None, Some y   -> Some y
        | None, None     -> None

    let getOrElse k v map = 
        match Map.tryFind k map with
        | None -> v
        | Some v -> v
   
type VTime = Map<ReplicaId, int64>

[<RequireQualifiedAccess>]     
module Version =  
    let zero: VTime = Map.empty
    let inc r (vv: VTime) = vv |> Helpers.upsert r 1L ((+)1L)
    let merge vv1 vv2 =
        vv2 |> Map.fold (fun acc k v2 -> Helpers.upsert k v2 (max v2) acc) vv1
    let compare (a: VTime) (b: VTime): Ord = 
        let valOrDefault k map =
            match Map.tryFind k map with
            | Some v -> v
            | None   -> 0L
        let akeys = a |> Map.toSeq |> Seq.map fst |> Set.ofSeq
        let bkeys = b |> Map.toSeq |> Seq.map fst |> Set.ofSeq
        (akeys + bkeys)
        |> Seq.fold (fun prev k ->
            let va = valOrDefault k a
            let vb = valOrDefault k b
            match prev with
            | Ord.Eq when va > vb -> Ord.Gt
            | Ord.Eq when va < vb -> Ord.Lt
            | Ord.Lt when va > vb -> Ord.Cc
            | Ord.Gt when va < vb -> Ord.Cc
            | _ -> prev ) Ord.Eq