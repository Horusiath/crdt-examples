/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

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
   
type VTime = Map<ReplicaId, uint64>

[<Interface>]
type IConvergent<'a> =
    abstract merge: 'a -> 'a -> 'a

[<RequireQualifiedAccess>]     
module Version =  

    let zero: VTime = Map.empty
    
    let inc r (vv: VTime): VTime = vv |> Helpers.upsert r 1UL ((+)1UL)

    let set r ts (vv: VTime): VTime = Map.add r ts vv

    let max (vv1: VTime) (vv2: VTime) =
        vv2 |> Map.fold (fun acc k v2 -> Helpers.upsert k v2 (max v2) acc) vv1
        
    let min (vv1: VTime) (vv2: VTime) =
        vv2 |> Map.fold (fun acc k v2 -> Helpers.upsert k v2 (min v2) acc) vv1
        
    let merge = max

    let compare (a: VTime) (b: VTime): Ord = 
        let valOrDefault k map =
            match Map.tryFind k map with
            | Some v -> v
            | None   -> 0UL
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

    [<Struct>]
    type Merge = 
        interface IConvergent<VTime> with
            member __.merge a b = merge a b 

/// Matrix clock to setup the version of an entity.
type MClock = Map<ReplicaId, VTime>

[<RequireQualifiedAccess>]
module MVersion =
    let zero: MClock = Map.empty

    let merge (replica: ReplicaId) (vtime: VTime) (clock: MClock): MClock =
        Helpers.upsert replica vtime (Version.merge vtime) clock

    let min (clock: MClock): VTime = clock |> Map.fold (fun acc k time -> Version.min acc time) Map.empty
    
    let max (clock: MClock): VTime = clock |> Map.fold (fun acc k time -> Version.max acc time) Map.empty

[<RequireQualifiedAccess>]
module Option =

    let merge nestedMerge a b =
        match a, b with
        | None, None -> None
        | left, None -> left
        | None, right -> right
        | Some left, Some right -> Some (nestedMerge left right)

    [<Struct>]
    type Merge<'a, 'm when 'm :> IConvergent<'a>> = 
        interface IConvergent<Option<'a>> with
            member __.merge a b = 
                let nestedMerge = Unchecked.defaultof<'m>.merge
                merge nestedMerge a b
                
[<RequireQualifiedAccess>]
module Array =
    
    let insert idx item array =
        let len = Array.length array
        let copy = Array.zeroCreate (len + 1)
        Array.blit array 0 copy 0 idx
        copy.[idx] <- item
        Array.blit array idx copy (idx+1) (len - idx)
        copy
        
    let removeAt idx array =
        let len = Array.length array
        let copy = Array.zeroCreate (len - 1)
        Array.blit array 0 copy 0 idx
        Array.blit array (idx+1) copy idx (len - idx - 1)
        copy
        
    let replace idx item array =
        let copy = Array.copy array
        copy.[idx] <- item
        copy
        
    /// Binary search for index in an ordered sequence, looking for a place to insert
    /// an element. Predicate can be used as eg. `fun a -> toInsert >= a`.
    /// If 'toInsert' is the lowest element, 0 will be returned. If it's the highest
    /// one: array.Length will be returned.
    let binarySearch (predicate: 'a -> bool) (array: 'a[]) =
        let mutable i = 0
        let mutable j = array.Length
        while i < j do
            let half = (i + j) / 2
            if not (predicate array.[half]) then i <- half + 1
            else j <- half
        i