/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Commutative.Pure

open System
open Akka.Actor
open Crdt
open Akkling

/// Operation - it carries user-defined operation wrapped into metadata
/// that enables determining order and redundancy of operations within partially-ordered log.
[<CustomComparison;CustomEquality>]
type Op<'a> =
    { Version: VTime      // version kept for keeping causal order
      Timestamp: DateTime // wallclock timestamp - useful for LWW and total ordering in case of concurrent versions
      Origin: ReplicaId   // replica where operation was submitted
      Value: 'a }         // operation-specific data submitted by user
    member this.Equals(other: Op<'a>) = this.CompareTo(other) = 0
    member this.CompareTo(other: Op<'a>) =
        match Version.compare this.Version other.Version with
        | Ord.Cc ->
            let cmp = this.Timestamp.CompareTo other.Timestamp
            if cmp = 0 then this.Origin.CompareTo other.Origin
            else cmp
        | cmp -> int cmp
    override this.Equals(obj) = match obj with :? Op<'a> as op -> this.Equals(op) | _ -> false
    override this.GetHashCode() = this.Version.GetHashCode()
    override this.ToString() = sprintf "Op<%O, %s, %O, %O>" this.Value this.Origin this.Timestamp.Ticks this.Version
    interface IEquatable<Op<'a>> with member this.Equals other = this.Equals other
    interface IComparable<Op<'a>> with member this.CompareTo other = this.CompareTo other
    interface IComparable with member this.CompareTo other =
        match other with
        | :? Op<'a> as t -> this.CompareTo t
        | _ -> failwithf "cannot compare Op to other structure"

/// Matrix clock - keeps information about all recently received versions from each replica.
type MTime = Map<ReplicaId, VTime>

[<RequireQualifiedAccess>]
module MTime =
    
  /// Returns vector version of all observed version in matrix clock, that's causally
  /// stable (meaning: there are no operations in flight in the system that could be
  /// concurrent to that vector version).
  let min (m: MTime) : VTime = Map.fold (fun acc _ -> Version.min acc) Version.zero m
  
  /// Returns the latest (most up to date) vector version as observed by this matrix clock. 
  let max (m: MTime) : VTime = Map.fold (fun acc _ -> Version.max acc) Version.zero m
  
  /// Merges two matrix clocks together returning matrix clock with most up to date values.
  let merge (m1: MTime) (m2: MTime) : MTime =
    (m1, m2) ||> Map.fold (fun acc k v ->
      match Map.tryFind k acc with
      | None -> Map.add k v acc
      | Some v2 -> Map.add k (Version.merge v v2) acc)
        
  /// Updates `version` observed at given `nodeId`, merging it with potentially already existing one.
  let update (nodeId: ReplicaId) (version: VTime) (m: MTime) =
    match Map.tryFind nodeId m with
    | None -> Map.add nodeId version m
    | Some v -> Map.add nodeId (Version.merge version v) m

/// Snapshot of a state at given replica, can be used in two scenarios:
/// 
/// 1. It should be persisted when stable state is stabilized.
/// 2. When new fresh node arrives, it's send before all unstable updates.
///
/// Snapshots can cause potential problems as malicious nodes could cause harm by
/// (on purpose or by accident) calling snapshots with versions concurrent to
/// stable timestamp. In that case data loss and/or node eviction may be necessary.
type Snapshot<'state> =
    { Stable: 'state       // stable state of the replica
      StableVersion: VTime // stable timestamp `Stable` state refers to
      LatestVersion: VTime // the latest version known to a given replica
      Observed: MTime }    // the observed universe, used to determine new stable checkpoints
    
type PureCrdt<'state, 'op> =
    /// Return default (zero) state. Used to initialize CRDT state.
    abstract Default: 'state
    /// Check if `old` operation makes `incoming` one redundant.
    abstract Obsoletes: old:Op<'op> * incoming:Op<'op> -> bool
    /// Apply `operations` to a given `state`. All `operations` are unstable. 
    abstract Apply: state:'state * operations:Set<Op<'op>> -> 'state

type Endpoint<'state,'op> = IActorRef<Protocol<'state, 'op>>

and Protocol<'state,'op> =
    | Query                                                 // get current state from received replica
    | Connect          of ReplicaId * Endpoint<'state,'op>  // connect to given replica
    | Replicate        of ReplicaId * VTime                 // request to replicate operations starting from given version
    | Reset            of from:ReplicaId * Snapshot<'state> // reset state at current replica to given snapshot
    | Replicated       of from:ReplicaId * Set<Op<'op>>     // replicate a batch of operations from given node
    | ReplicateTimeout of ReplicaId                         // `Replicate` request has timed out
    | Submit           of 'op                               // submit a new operation
    //| Evict            of ReplicaId                       // kick out node from cluster, its replication events will no longer be validated 
    //| Evicted          of ReplicaId * stable:VTime        // node was kicked out from the cluster, because it had invalid timestamp
    
type State<'state,'op> =
    { Id: ReplicaId          // Id of current replica.
      Stable: 'state         // Stable operations.
      Unstable: Set<Op<'op>> // Unstable operations waiting to stabilize.
      StableVersion: VTime   // the last known stable version
      LatestVersion: VTime   // the most up-to-date version recognized by current node
      Observed: MTime        // Matrix clock of all observed vector clocks received from incoming replicas.
      Connections: Map<ReplicaId, (Endpoint<'state, 'op> * ICancelable)> } // Active connections to other replicas.
    
module Replicator =
    
    /// Time window in which replicate request are retried.
    let replicateTimeout = TimeSpan.FromMilliseconds 200.
    
    let refreshTimeout nodeId (state: State<_,_>) (ctx: Actor<_>) =
        let (target, timeout) = Map.find nodeId state.Connections
        timeout.Cancel()
        let timeout = ctx.Schedule replicateTimeout ctx.Self (ReplicateTimeout nodeId)
        { state with Connections = Map.add nodeId (target, timeout) state.Connections }
    
    /// Stabilize current state. It means computing the latest stable timestamp based on current
    /// knowledge of the system and determining which unstable operations can be considered
    /// stable according to new stable timestamp. 
    let stabilize (state: State<'state,'op>) =
        let stableTimestamp = MTime.min state.Observed    
        let stable, unstable =
            state.Unstable
            |> Set.partition (fun op -> Version.compare op.Version stableTimestamp <= Ord.Eq)
        (stable, unstable, stableTimestamp)
        
    /// Generates serializable snapshot of a current state. 
    let toSnapshot (state: State<_,_>) =
        { Stable = state.Stable
          LatestVersion = state.LatestVersion
          StableVersion = state.StableVersion
          Observed = state.Observed }
        
    /// Applies all unstable operations on top of stable state, returning the most actual
    /// state as know by current node.
    let apply (crdt: PureCrdt<'state,'op>) (state: State<'state,'op>) =
        if Set.isEmpty state.Unstable
        then state.Stable
        else crdt.Apply(state.Stable, state.Unstable)
    
    let actor (crdt: PureCrdt<'state,'op>) (id: ReplicaId) (ctx: Actor<Protocol<'state,'op>>) =
        let rec active (crdt: PureCrdt<'state,'op>) (state: State<'state,'op>) (ctx: Actor<Protocol<'state,'op>>) = actor {
            match! ctx.Receive() with
            | Connect(nodeId, target) ->
                //target <! Replicate(state.Id, state.LatestVersion)
                Map.tryFind nodeId state.Connections |> Option.iter (fun (_, t) -> t.Cancel())
                let timeout = ctx.Schedule replicateTimeout ctx.Self (ReplicateTimeout nodeId)
                let state = { state with Connections = Map.add nodeId (target, timeout) state.Connections }
                return! active crdt state ctx
                
            | Query ->
                let result = apply crdt state
                ctx.Sender() <! result            
                logDebugf ctx "query (stable: %O, unstable: %O) => %O" state.Stable state.Unstable result
                return! active crdt state ctx
                
            | Replicate(nodeId, filter) ->
                let (replyTo, _) = Map.find nodeId state.Connections
                match Version.compare filter state.StableVersion with
                | Ord.Lt -> replyTo <! Reset(id, toSnapshot state) // send reset request if remote version is behind our stable
                | _ -> ()
                
                let ops =
                    state.Unstable
                    |> Set.filter (fun op -> Version.compare op.Version filter > Ord.Eq) // get greater than or concurrent versions
                replyTo <! Replicated(id, ops)
                
                logDebugf ctx "received replicate request with timestamp %O. Sending %i ops." filter (Set.count ops)
                return! active crdt state ctx
            
            | ReplicateTimeout nodeId ->
                logDebugf ctx "replicate request timed out. Retrying."
                let (target,_) = Map.find nodeId state.Connections
                target <! Replicate(state.Id, state.LatestVersion)
                let state = refreshTimeout nodeId state ctx 
                return! active crdt state ctx
            
            | Reset(nodeId, snapshot) when Version.compare snapshot.StableVersion state.LatestVersion = Ord.Gt ->
                logDebugf ctx "reset state from %O: %O" nodeId snapshot.StableVersion 
                let state =
                    { state with
                        Stable = snapshot.Stable
                        StableVersion = snapshot.StableVersion
                        LatestVersion = Version.merge state.LatestVersion snapshot.StableVersion
                        Observed = MTime.merge snapshot.Observed state.Observed }
                return! active crdt state ctx
                    
            | Reset(nodeId, snapshot) ->
                logErrorf ctx "received Reset from %O with version: %O (local version: %O). Ignoring." nodeId snapshot.StableVersion state.LatestVersion
                return! active crdt state ctx
               
            | Replicated(nodeId, ops) when Set.isEmpty ops ->
                let state = refreshTimeout nodeId state ctx
                return! active crdt state ctx
                        
            | Replicated(nodeId, ops) ->
                let mutable state = state
                
                for op in ops |> Set.filter (fun op -> Version.compare op.Version state.LatestVersion > Ord.Eq) do
                    let observed = state.Observed |> MTime.update nodeId op.Version
                    // check if new incoming operation is not obsolete
                    let obsolete = state.Unstable |> Set.exists(fun o -> crdt.Obsoletes(o, op))
                    // prune unstable operations, which have been obsoleted by incoming operation
                    let pruned = state.Unstable |> Set.filter (fun o -> not (crdt.Obsoletes(op, o)))
                    state <- { state with
                                 Observed = observed
                                 Unstable = if obsolete then pruned else Set.add op pruned
                                 LatestVersion = Version.merge op.Version state.LatestVersion }
                
                // check if some of unstable operation have stabilized now    
                let stableOps, unstableOps, stableVersion = stabilize state
                let state =
                    if not (Set.isEmpty stableOps) then
                        logDebugf ctx "stabilized state %A over %O (stable: %O, unstable: %O)" state.Stable stableVersion stableOps unstableOps
                        let stable = crdt.Apply(state.Stable, stableOps)
                        { state with Stable = stable; Unstable = unstableOps; StableVersion = stableVersion }
                    else
                        state
                        
                let state = refreshTimeout nodeId state ctx
                return! active crdt state ctx
            
            | Submit op ->
                let sender = ctx.Sender()
                let version = Version.inc id state.LatestVersion
                let versioned =
                    { Version = version
                      Value = op
                      Timestamp = DateTime.UtcNow
                      Origin = id }
                // prune unstable operations, which have been obsoleted by this operation
                let pruned = state.Unstable |> Set.filter (fun o -> not (crdt.Obsoletes(versioned, o)))
                let observed = state.Observed |> MTime.update id version
                let state = { state with Unstable = Set.add versioned pruned; LatestVersion = version; Observed = observed }
                
                // respond to a called with new state (not necessary but good for testing)
                let reply = apply crdt state
                logDebugf ctx "submitted operation %O at timestamp %O - state after application: %A" op version reply 
                sender <! reply
                return! active crdt state ctx }
        
        let state =
            { Id = id
              Stable = crdt.Default
              StableVersion = Version.zero
              LatestVersion = Version.zero
              Unstable = Set.empty
              Observed = Map.empty
              Connections = Map.empty }
        active crdt state ctx