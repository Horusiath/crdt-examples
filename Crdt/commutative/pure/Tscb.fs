namespace Crdt.Commutative.Pure

open System
open Akka.Actor
open Crdt
open Akkling
open FSharp.Control

/// Operation - it carries user data wrapped into metadata that enables determining order of redundant operations
/// within partially-ordered log.
[<CustomComparison;CustomEquality>]
type Versioned<'a> =
    { Version: VTime
      Timestamp: DateTime
      Origin: ReplicaId
      Value: 'a }
    member this.Equals(other: Versioned<'a>) = this.CompareTo(other) = 0
    member this.CompareTo(other: Versioned<'a>) =
        match Version.compare this.Version other.Version with
        | Ord.Cc ->
            let cmp = this.Timestamp.CompareTo other.Timestamp
            if cmp = 0 then this.Origin.CompareTo other.Origin
            else cmp
        | cmp -> int cmp
    override this.Equals(obj) = match obj with :? Versioned<'a> as op -> this.Equals(op) | _ -> false
    override this.GetHashCode() = this.Version.GetHashCode()
    override this.ToString() = sprintf "Versioned<%O, %s, %O, %O>" this.Value this.Origin this.Timestamp.Ticks this.Version
    interface IEquatable<Versioned<'a>> with member this.Equals other = this.Equals other
    interface IComparable<Versioned<'a>> with member this.CompareTo other = this.CompareTo other
    interface IComparable with member this.CompareTo other =
        match other with
        | :? Versioned<'a> as t -> this.CompareTo t
        | _ -> failwithf "cannot compare Op to other structure"

type MTime = Map<ReplicaId, VTime>

[<RequireQualifiedAccess>]
module MTime =
    
  let min (m: MTime) : VTime = Map.fold (fun acc _ -> Version.min acc) Version.zero m
  let max (m: MTime) : VTime = Map.fold (fun acc _ -> Version.max acc) Version.zero m
  
  let merge (m1: MTime) (m2: MTime) : MTime =
    (m1, m2) ||> Map.fold (fun acc k v ->
      match Map.tryFind k acc with
      | None -> Map.add k v acc
      | Some v2 -> Map.add k (Version.merge v v2) acc)
        
  let update (nodeId: ReplicaId) (version: VTime) (m: MTime) =
    match Map.tryFind nodeId m with
    | None -> Map.add nodeId version m
    | Some v -> Map.add nodeId (Version.merge version v) m

type Snapshot<'state> =
    { Stable: Versioned<'state>
      Observed: MTime }
    
type PureCrdt<'state, 'op> =
    /// Return default (zero) state. Used to initialize CRDT state.
    abstract Default: 'state
    /// Check if `incoming` operation makes `stable` state obsolete.
    abstract Prune: incoming:Versioned<'op> * stable:'state * stableTimestamp:VTime -> bool
    /// Check if `old` operation makes `incoming` one redundant.
    abstract Obsoletes: old:Versioned<'op> * incoming:Versioned<'op> -> bool
    /// Apply operation to a given state.
    abstract Apply: state:'state * op:'op -> 'state

type Endpoint<'state,'op> = IActorRef<Protocol<'state, 'op>>

and Protocol<'state,'op> =
    | Query                                                 // get current state from received replica
    | Connect          of ReplicaId * Endpoint<'state,'op>  // connect to given replica
    | Replicate        of ReplicaId * VTime                 // request to replicate operations starting from given vtime
    | Reset            of from:ReplicaId * Snapshot<'state> // reset state at current replica to given snapshot
    | Replicated       of from:ReplicaId * Versioned<'op>[] // replicate a batch of operations from given node
    | ReplicateTimeout of ReplicaId                         // `Replicate` request has timed out
    | Submit           of 'op                               // submit a new operation
    //| Evict            of ReplicaId                       // kick out node from cluster, its replication events will no longer be validated 
    //| Evicted          of ReplicaId * stable:VTime        // node was kicked out from the cluster, because it had invalid timestamp
    
type State<'state,'op> =
    { /// Id of current replica.
      Id: ReplicaId
      /// The stable timestamp.
      Stable: Versioned<'state>
      // Unstable operations waiting to stabilize.
      Unstable: Set<Versioned<'op>>
      LatestVersion: VTime
      /// Matrix clock of all observed vector clocks received from incoming replicas.
      Observed: MTime
      /// Active connections to other replicas.
      Connections: Map<ReplicaId, (Endpoint<'state, 'op> * ICancelable)> }
 
type DbEntry<'state,'op> =
    | Snapshot of Snapshot<'state>
    | Op of Versioned<'op>
        
type Db =
    abstract GetSnapshot: unit -> Async<Snapshot<'state> option>
    abstract GetOperations: filter:VTime -> AsyncSeq<Versioned<'op>>
    abstract Store: DbEntry<'state,'op> seq -> Async<unit>
    abstract Prune: VTime -> Async<unit>
    
module Replicator =
    
    let replicateTimeout = TimeSpan.FromMilliseconds 200.
    
    let refreshTimeout nodeId (state: State<_,_>) (ctx: Actor<_>) =
        let (target, timeout) = Map.find nodeId state.Connections
        timeout.Cancel()
        let timeout = ctx.Schedule replicateTimeout ctx.Self (ReplicateTimeout nodeId)
        { state with Connections = Map.add nodeId (target, timeout) state.Connections }
        
    let stabilize (state: State<'state,'op>) =
        let stableTimestamp = state.Observed |> MTime.min        
        let stable, unstable =
            state.Unstable
            |> Set.partition (fun op -> Version.compare op.Version stableTimestamp <= Ord.Eq)
        (stable, unstable, stableTimestamp)
    
    let actor (crdt: PureCrdt<'state,'op>) (db: Db) (id: ReplicaId) (ctx: Actor<Protocol<'state,'op>>) =
        let rec active (crdt: PureCrdt<'state,'op>) (db: Db) (state: State<'state,'op>) (ctx: Actor<Protocol<'state,'op>>) = actor {
            match! ctx.Receive() with
            | Connect(nodeId, target) ->
                target <! Replicate(state.Id, state.LatestVersion)
                Map.tryFind nodeId state.Connections |> Option.iter (fun (_, t) -> t.Cancel())
                let timeout = ctx.Schedule replicateTimeout ctx.Self (ReplicateTimeout nodeId)
                let state = { state with Connections = Map.add nodeId (target, timeout) state.Connections }
                return! active crdt db state ctx
                
            | Query ->
                let reply = state.Unstable |> Set.fold (fun acc op -> crdt.Apply(acc, op.Value)) state.Stable.Value
                ctx.Sender() <! reply                
                logDebugf ctx "query (stable: %O, unstable: %O) => %O" state.Stable state.Unstable reply
                return! active crdt db state ctx
                
            | Replicate(nodeId, filter) ->
                let (replyTo, _) = Map.find nodeId state.Connections
                match Version.compare filter state.Stable.Version with
                | Ord.Lt -> replyTo <! Reset(id, { Stable = state.Stable; Observed = state.Observed })
                | _ -> ()
                
                let ops =
                    state.Unstable
                    |> Seq.filter (fun op -> Version.compare op.Version filter > Ord.Eq) // get greater than or concurrent versions
                    |> Seq.toArray
                replyTo <! Replicated(id, ops)
                
                logDebugf ctx "received replicate request with timestamp %O. Sending %i ops." filter (Array.length ops)
                return! active crdt db state ctx
            
            | ReplicateTimeout nodeId ->
                logDebugf ctx "replicate request timed out. Retrying"
                let (target,_) = Map.find nodeId state.Connections
                target <! Replicate(state.Id, state.LatestVersion)
                let state = refreshTimeout nodeId state ctx 
                return! active crdt db state ctx
            
            | Reset(nodeId, snapshot) when Version.compare snapshot.Stable.Version state.LatestVersion = Ord.Gt ->
                logDebugf ctx "reset state from %O: %O" nodeId snapshot.Stable.Version 
                let state =
                    { state with
                        Stable = snapshot.Stable
                        Observed = MTime.merge snapshot.Observed state.Observed }
                return! active crdt db state ctx
                    
            | Reset(nodeId, snapshot) ->
                logErrorf ctx "received Reset from %O with version: %O (local version: %O). Ignoring." nodeId snapshot.Stable.Version state.LatestVersion
                return! active crdt db state ctx
                        
            | Replicated(nodeId, ops) ->
                let mutable state = state
                let batch = ResizeArray()
                for op in ops |> Array.filter (fun op -> Version.compare op.Version state.LatestVersion > Ord.Eq) do
                    let observed = state.Observed |> MTime.update nodeId op.Version
                    let incomingObsolete = state.Unstable |> Set.exists(fun o -> crdt.Obsoletes(o, op))
                    let stableObsolete = crdt.Prune(op, state.Stable.Value, state.Stable.Version)
                    let unstable = if incomingObsolete then state.Unstable else Set.add op state.Unstable
                    state <- { state with
                                 Observed = observed
                                 Unstable = unstable
                                 LatestVersion = Version.merge op.Version state.LatestVersion }
                    logDebugf ctx "received %s operation: %O" (if incomingObsolete then "redundant" else "actual") op 
                    batch.Add (Op op)
                let stabilized, unstable, stableVersion = stabilize state
                let state =
                    if not (Set.isEmpty stabilized) then
                        logDebugf ctx "stabilized over %O (stable: %O, unstable: %i)" stableVersion (Set.count stabilized) (Set.count unstable) 
                        let s = stabilized |> Set.fold (fun acc op -> crdt.Apply(acc, op.Value)) state.Stable.Value
                        let date = stabilized |> Seq.map (fun op -> op.Timestamp) |> Seq.max
                        let stable = { Version = stableVersion; Timestamp = date; Origin = id; Value = s }
                        batch.Add (Snapshot { Stable = stable; Observed = state.Observed })
                        { state with Unstable = unstable; Stable = stable }
                    else
                        state
                do! db.Store batch
                do! db.Prune stableVersion
                let state = refreshTimeout nodeId state ctx
                return! active crdt db state ctx
            
            | Submit op ->
                let sender = ctx.Sender()
                let version = Version.inc id state.LatestVersion
                let versioned =
                    { Version = version
                      Value = op
                      Timestamp = DateTime.UtcNow
                      Origin = id }
                do! db.Store [|Op versioned|]
                let state = { state with Unstable = Set.add versioned state.Unstable; LatestVersion = version }
                let reply = state.Unstable |> Set.fold (fun acc op -> crdt.Apply(acc, op.Value)) state.Stable.Value
                logDebugf ctx "submitted operation %O at timestamp %O - state after application: %O" op version reply 
                sender <! reply
                return! active crdt db state ctx }
        
        and recovering (crdt: PureCrdt<'state,'op>) (db: Db) (state: State<'state,'op>) (ctx: Actor<Protocol<'state,'op>>) = actor {
            match! ctx.Receive() with
            | Reset(sender, snapshot) when sender = state.Id ->
                let state =
                    { state with
                        Stable = snapshot.Stable
                        Observed = snapshot.Observed }
                return! recovering crdt db state ctx
            | Replicated(sender, ops) when sender = state.Id ->
                let state =
                    ops
                    |> Array.fold (fun acc op ->
                        if op.Origin = state.Id then
                            { acc with Unstable = Set.add op acc.Unstable; LatestVersion = Version.merge op.Version acc.LatestVersion }
                        else
                            { acc with
                                Unstable = Set.add op acc.Unstable
                                Observed = MTime.update op.Origin op.Version acc.Observed }
                    ) state
                return! recovering crdt db state ctx
            | ReplicateTimeout sender when sender = state.Id ->
                ctx.UnstashAll()
                return! active crdt db state ctx
            | _ ->
                ctx.Stash()
                return! recovering crdt db state ctx }
        
        Async.Start(async {
            let! snapshot = db.GetSnapshot()
            snapshot |> Option.iter (fun s -> ctx.Self <! Reset(id, s))
            let buf = ResizeArray 30
            for op in db.GetOperations Version.zero do
                buf.Add op
                if buf.Count >= 30 then
                    ctx.Self <! Replicated(id, buf.ToArray())
                    buf.Clear()
            ctx.Self <! ReplicateTimeout id // we use timeout to commit the recovery phase
        })
        
        let init = { Value = crdt.Default; Version = Version.zero; Timestamp = DateTime.MinValue; Origin = id }
        let state =
            { Id = id
              Stable = init
              LatestVersion = Version.zero
              Unstable = Set.empty
              Observed = Map.empty
              Connections = Map.empty }
        recovering crdt db state ctx