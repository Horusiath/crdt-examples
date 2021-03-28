/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative.Pure

open System
open Crdt

open FSharp.Control

/// Operation - it carries user data wrapped into metadata that enables determining order of redundant operations
/// within partially-ordered log.
//[<CustomComparison;CustomEquality>]
//type Op<'a> =
//    { Version: VTime
//      Timestamp: DateTime
//      Origin: ReplicaId
//      Value: 'a }
//    member this.Equals(other: Op<'a>) = this.CompareTo(other) = 0
//    member this.CompareTo(other: Op<'a>) =
//        match Version.compare this.Version other.Version with
//        | Ord.Eq | Ord.Cc ->
//            let cmp = this.Timestamp.CompareTo other.Timestamp
//            if cmp = 0 then this.Origin.CompareTo other.Origin
//            else cmp
//        | cmp -> int cmp
//    override this.Equals(obj) = match obj with :? Op<'a> as op -> this.Equals(op) | _ -> false
//    override this.GetHashCode() = this.Version.GetHashCode()
//    interface IEquatable<Op<'a>> with member this.Equals other = this.Equals other
//    interface IComparable<Op<'a>> with member this.CompareTo other = this.CompareTo other
//    interface IComparable with member this.CompareTo other =
//        match other with
//        | :? Op<'a> as t -> this.CompareTo t
//        | _ -> failwithf "cannot compare Op to other structure"
//
//type MTime = Map<ReplicaId, VTime>
//
//[<RequireQualifiedAccess>]
//module MTime =
//    
//  let min (m: MTime) : VTime = Map.fold (fun acc _ -> Version.min acc) Version.zero m
//  let max (m: MTime) : VTime = Map.fold (fun acc _ -> Version.max acc) Version.zero m
//  
//  let merge (m1: MTime) (m2: MTime) : MTime =
//    (m1, m2) ||> Map.fold (fun acc k v ->
//      match Map.tryFind k acc with
//      | None -> Map.add k v acc
//      | Some v2 -> Map.add k (Version.merge v v2) acc)
//        
//  let update (nodeId: ReplicaId) (version: VTime) (m: MTime) =
//    match Map.tryFind nodeId m with
//    | None -> Map.add nodeId version m
//    | Some v -> Map.add nodeId (Version.merge version v) m
//
//type Snapshot<'state> =
//    { Value: 'state
//      Version: VTime
//      Timestamp: DateTime
//      SeqNr: uint64 }
//    
//type DbEntry<'s,'op> =
//    | Snapshot of Snapshot<'s>
//    | Op of Op<'op>
//    
//[<Interface>]           
//type Db =
//    abstract GetSnapshot: unit -> Async<Snapshot<'s> option>
//    /// Get entries greater than or equal to sequence nr. Snapshots
//    /// are always returned first if they meet sequence nr. criteria.
//    abstract GetOps: seqNr:uint64 -> AsyncSeq<Op<'op>>
//    /// Override latest snapshot with the new one.
//    abstract PutBatch: DbEntry<'s,'op> seq -> Async<unit>
//    /// Remove all entries with sequence number lower than `seqNr` with timestamps lower than `filter`. 
//    abstract Prune: seqNr:uint64 * filter:VTime -> Async<unit> 
//
//[<Interface>]
//type PureCrdt<'state, 'op> =
//    /// Return default (zero) state. Used to initialize CRDT state.
//    abstract Default: 'state
//    /// Check if given operation is obsoleted for a given state.
//    abstract Obsolete: stable:Snapshot<'state> * op:Op<'op> -> bool
//    /// Check if given operations `o2` has been obsoleted by `o1`.
//    abstract Obsolete: o1:Op<'op> * o2:Op<'op> -> bool
//    /// Apply operation to a given state.
//    abstract Apply: state:'state * op:Op<'op> -> 'state