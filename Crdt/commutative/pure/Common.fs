/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative.Pure

open System
open Crdt

open FSharp.Control

/// Operation - it carries user data wrapped into metadata that enables determining order of redundant operations
/// within partially-ordered log.
[<Struct; CustomComparison;CustomEquality>]
type Op<'a> =
    { Version: VTime
      Timestamp: DateTime
      Origin: ReplicaId
      Value: 'a }
    member this.Equals(other: Op<'a>) = this.CompareTo(other) = 0
    member this.CompareTo(other: Op<'a>) =
        match Version.compare this.Version other.Version with
        | Ord.Eq | Ord.Cc ->
            let cmp = this.Timestamp.CompareTo other.Timestamp
            if cmp = 0 then this.Origin.CompareTo other.Origin
            else cmp
        | cmp -> int cmp
    override this.Equals(obj) = match obj with :? Op<'a> as op -> this.Equals(op) | _ -> false
    interface IEquatable<Op<'a>> with member this.Equals other = this.Equals other
    interface IComparable<Op<'a>> with member this.CompareTo other = this.CompareTo other
    interface IComparable with member this.CompareTo other =
        match other with
        | :? Op<'a> as t -> this.CompareTo t
        | _ -> failwithf "cannot compare Op to other structure"

type POLog<'a> = Set<Op<'a>>

/// Returns true if the first operation makes second one redundant.
type Redundancy<'a> = Op<'a> -> Op<'a> -> bool

/// A product of three operations:
/// 1. `r` defines whether the delivered operation is itself redundant and does not need to be added itself to the PO-Log.
/// 2. `r0` is used when the new delivered operation is discarded being redundant.
/// 3. `r1` is used if the new delivered operation is added to the PO-Log.
type CausalRedundancy<'a> = (Op<'a> -> POLog<'a> -> bool) * Redundancy<'a> * Redundancy<'a>

[<RequireQualifiedAccess>]
module POLog =
    
    let inline prune (isRedundant: Redundancy<'a>) (op: Op<'a>) (log: POLog<'a>) =
        Set.filter (fun t -> not (isRedundant t op)) log
        
    let add ((r, r0, r1): CausalRedundancy<'a>) (op: Op<'a>) (log: POLog<'a>) =
        let redundant = r op log
        if redundant then
            prune r0 op log
        else
            prune r1 op (Set.add op log)
            
    let stable (vv: VTime) (log: POLog<'a>) =
        let inline isStable op =
            match Version.compare op.Version vv with
            | Ord.Lt | Ord.Eq -> true
            | _ -> false
        let stable, unstable = Set.partition isStable log 
        (stable, unstable)

[<Interface>]
type Crdt<'crdt, 'state, 'op> =
    abstract Default: 'crdt
    abstract Query: state:'crdt -> 'state
    abstract Effect: state:'crdt * operation:Op<'op> -> 'crdt
    abstract Redundancy: CausalRedundancy<'op>    
    
[<RequireQualifiedAccess>]
module Counter =
    
    let private r op log = failwith "not impl"
    let private r0 : Redundancy<'a> = fun a b -> Version.compare a.Version b.Version = Ord.Lt
        
    let crdt =
        { new Crdt<int64, int64, int64> with
            member _.Default = 0L
            member _.Query crdt = crdt
            member _.Effect (state, op) = state + op.Value
            member _.Redundancy = (r, r0, r0) }
        
[<RequireQualifiedAccess>]
module LWWReg =
    
    [<Struct>]
    type LWWRegister<'a> =
        { Timestamp: struct(DateTime * ReplicaId)
          Value: 'a voption }
        
    let private r op log = failwith "not impl"
    let private r0 : Redundancy<'a> = fun a b -> Version.compare a.Version b.Version = Ord.Lt
        
    let crdt =
        { new Crdt<LWWRegister<'a>, 'a voption, 'a> with
            member _.Default = failwith "not impl"
            member _.Query crdt = failwith "not impl" 
            member _.Effect (state, op) = failwith "not impl"
            member _.Redundancy = (r, r0, r0) }
        
[<RequireQualifiedAccess>]
module MVReg =
    
    [<Struct>]
    type MVRegister<'a> =
        { Timestamp: struct(DateTime * ReplicaId)
          Value: 'a voption }
        
    let private r op log = failwith "not impl"
    let private r0 : Redundancy<'a> = fun a b -> Version.compare a.Version b.Version = Ord.Lt
        
    let crdt =
        { new Crdt<MVRegister<'a>, 'a voption, 'a> with
            member _.Default = failwith "not impl"
            member _.Query crdt = failwith "not impl" 
            member _.Effect (state, op) = failwith "not impl"
            member _.Redundancy = (r, r0, r0) }
        
[<RequireQualifiedAccess>]
module ORSet =
    
    type ORSet<'a> = Set<Op<'a>>
    
    type Operation<'a> =
        | Add of 'a
        | Remove of 'a
        
    let private r op log = failwith "not impl"
    let private r0 : Redundancy<'a> = fun a b -> Version.compare a.Version b.Version = Ord.Lt
        
    let crdt =
        { new Crdt<ORSet<'a>, Set<'a>, Operation<'a>> with
            member _.Default = failwith "not impl"
            member _.Query crdt = failwith "not impl" 
            member _.Effect (state, op) = failwith "not impl"
            member _.Redundancy = (r, r0, r0) }