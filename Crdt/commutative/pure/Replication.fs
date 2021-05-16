/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Commutative.Pure

open System
open System.Diagnostics
open Akka.Actor
open Crdt
open Akkling
open Crdt

/// Event that carries user-defined operation wrapped into metadata
/// that enables determining order and redundancy of operations within partially-ordered log.
[<CustomComparison;CustomEquality>]
type Event<'a> =
  { Version: VTime      // version kept for keeping causal order
    Timestamp: DateTime // wall clock timestamp - useful for LWW and total ordering in case of concurrent versions
    Origin: ReplicaId   // replica which originally generated an event
    Value: 'a }         // CRDT-specific operation submitted by user
  member this.Equals(other: Event<'a>) = this.CompareTo(other) = 0
  member this.CompareTo(other: Event<'a>) =
    match Version.compare this.Version other.Version with
    | Ord.Cc ->
      let cmp = this.Timestamp.CompareTo other.Timestamp
      if cmp = 0 then this.Origin.CompareTo other.Origin
      else cmp
    | cmp -> int cmp
  override this.Equals(obj) = match obj with :? Event<'a> as op -> this.Equals(op) | _ -> false
  override this.GetHashCode() = this.Version.GetHashCode()
  override this.ToString() = sprintf "Op<%O, %s, %O, %O>" this.Value this.Origin this.Timestamp.Ticks this.Version
  interface IEquatable<Event<'a>> with member this.Equals other = this.Equals other
  interface IComparable<Event<'a>> with member this.CompareTo other = this.CompareTo other
  interface IComparable with member this.CompareTo other =
    match other with
    | :? Event<'a> as t -> this.CompareTo t
    | _ -> failwithf "cannot compare Op to other structure"

/// Matrix clock - keeps information about all recently received versions from each replica.
type MTime = Map<ReplicaId, VTime>

[<RequireQualifiedAccess>]
module MTime =
  
  /// Returns vector version of all observed version in matrix clock, that's causally
  /// stable (meaning: there are no events in flight in the system that could be
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
    | Some v ->
      if Version.compare version v > Ord.Eq then
        Map.add nodeId (Version.merge version v) m
      else
        m // if remove `version` is <= our local observation of it, don't do anything

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
    Observed: MTime      // the observed universe, used to determine new stable checkpoints
    Evicted: MTime }     // evicted nodes
  
type PureCrdt<'state, 'op> =
  /// Return default (zero) state. Used to initialize CRDT state.
  abstract Default: 'state
  /// Check if `old` event makes `incoming` one redundant.
  abstract Obsoletes: old:Event<'op> * incoming:Event<'op> -> bool
  /// Apply `operations` to a given `state`. All `operations` are unstable and never empty. 
  abstract Apply: state:'state * operations:Set<Event<'op>> -> 'state

/// Configuration which lets us define which specific kind of data are we interested in
/// when sending a query to obtain the CRDT state.
type QueryMode =
  /// Return the latest state version available. This state may not be
  /// consistent among other replicas as the replication is still in progress.
  | Latest
  /// Returns the stable state - consistent among all replicas. If any replica
  /// is disconnected, the stable state won't be able to move forward to `Latest`
  /// until it reconnects or will be evicted.
  | Stable
  
/// Notifications will be send to all actors, which subscribe to a Replicator.
type Notification<'state,'op> =
  /// Once a new stable timestamp is reached, this notification will be send.
  | Stable  of state:'state * version:VTime
  /// Notification send each time a new emitted or replicated event will arrive
  /// and be processed on the local node.
  | Emitted of Event<'op>
  /// This notification will be send only when a node had sent some events
  /// which were not replicated to another node, which evicted it. In that case
  /// concurrent events from evicted node will be revoked.
  | Revoked of Event<'op>
  /// Notification send prior to potential `Revoked` events, send when node
  /// was decided to be evicted.
  | Evicted of ReplicaId * VTime
  
type Subscriber<'state,'op> = IActorRef<Notification<'state,'op>>

type Endpoint<'state,'op> = IActorRef<Protocol<'state, 'op>>

and Protocol<'state,'op> =
  | Query            of QueryMode                         // get current state from received replica
  | Connect          of ReplicaId * Endpoint<'state,'op>  // connect to given replica
  | Replicate        of ReplicaId * VTime                 // request to replicate events starting from given version
  | Reset            of from:ReplicaId * Snapshot<'state> // reset state at current replica to given snapshot
  | Replicated       of from:ReplicaId * Set<Event<'op>>  // replicate a batch of events from given node
  | ReplicateTimeout of ReplicaId                         // `Replicate` request has timed out
  | Submit           of 'op                               // submit a new CRDT-specific operation
  | Evict            of ReplicaId                         // kick out node from cluster, its replication events will no longer be validated 
  | Evicted          of ReplicaId * VTime                 // node was kicked out from the cluster, because it had invalid timestamp
  | Subscribe        of Subscriber<'state,'op>            // subscribe to `Notification`s send by target replicator 
  | Unsubscribe      of Subscriber<'state,'op>            // unsubscribe from `Notification`s send by target replicator

type EvictionEvent =
  | Responsive of TimeSpan
  | TimedOut   of TimeSpan

/// Simplistic eviction policy API: a function which may internally hide some state.
/// This function takes `Responsive` event in case when observed endpoint responded,
/// or `TimedOut` event in case when replication request was not delivered.
/// Return None to evict given node. Return new timeout value that will be given for
/// a node to respond for the next request.
type EvictionPolicy = EvictionEvent -> TimeSpan option
  
/// Status of a particular connection between two nodes.
type ConnectionStatus<'state,'op> =
  { Id: ReplicaId
    Endpoint: Endpoint<'state,'op>
    Timeout: ICancelable
    Timer: Stopwatch
    EvictionPolicy: EvictionPolicy }
  
type Config =
  { Id: ReplicaId
    DefaultReplicationTimeout: TimeSpan
    EvictionPolicy: ReplicaId -> EvictionPolicy }
  
type State<'state,'op> =
  { Id: ReplicaId             // Id of current replica.
    Stable: 'state            // Stable state.
    Unstable: Set<Event<'op>> // Unstable operations waiting to stabilize.
    StableVersion: VTime      // the last known stable version
    LatestVersion: VTime      // the most up-to-date version recognized by current node
    Observed: MTime           // Matrix clock of all observed vector clocks received from incoming replicas.
    Evicted: MTime            // Versions at which particular nodes were evicted
    Subscribers: Set<Subscriber<'state,'op>> // set of subscribers for data notifications
    Connections: Map<ReplicaId, ConnectionStatus<'state,'op>> } // Active connections to other replicas.
  
module Replicator =
  
  let private processEviction (ctx: Actor<_>) state (connection: ConnectionStatus<_,_>) e =
    let nodeId = connection.Id
    match connection.EvictionPolicy e with
    | Some replicateTimeout ->
      logDebugf ctx "Refreshing timeout for node %s: %O" nodeId replicateTimeout
      connection.Timer.Restart()
      connection.Timeout.Cancel()
      let timeout = ctx.Schedule replicateTimeout ctx.Self (ReplicateTimeout nodeId)
      { state with Connections = Map.add nodeId { connection with Timeout = timeout } state.Connections }
    | None ->
      logDebugf ctx "Marking node %s to eviction" nodeId
      ctx.Self <! Evict nodeId
      state
  
  let private refreshTimeout nodeId (state: State<_,_>) (ctx: Actor<_>) =
    let connection = Map.find nodeId state.Connections
    let elapsed = connection.Timer.Elapsed
    processEviction ctx state connection (Responsive elapsed)
    
  /// Sends `notification` to all subscribers.
  let private notify (state: State<'state,'op>) (notification: Notification<'state,'op>)  =
    for subscriber in state.Subscribers do
      subscriber <! notification
  
  /// Stabilize current state. It means computing the latest stable timestamp based on current
  /// knowledge of the system and determining which unstable operations can be considered
  /// stable according to new stable timestamp. 
  let private stabilize ctx (crdt: PureCrdt<_,_>) (state: State<'state,'op>) =
    let stableVersion = MTime.min state.Observed  
    let stable, unstable =
      state.Unstable
      |> Set.partition (fun op -> Version.compare op.Version stableVersion <= Ord.Eq)
    if not (Set.isEmpty stable) then
      let stable = crdt.Apply(state.Stable, stable)
      logDebugf ctx "stabilized over %O (state: %O) -> %O (state: %O)" state.StableVersion state.Stable stableVersion stable
      notify state (Stable(stable, stableVersion))
      { state with Stable = stable; Unstable = unstable; StableVersion = stableVersion }
    else
      state
    
  /// Generates serializable snapshot of a current state. 
  let private toSnapshot (state: State<_,_>) =
    { Stable = state.Stable
      LatestVersion = state.LatestVersion
      StableVersion = state.StableVersion
      Observed = state.Observed
      Evicted = state.Evicted }
    
  /// Applies all unstable operations on top of stable state, returning the most actual
  /// state as know by current node.
  let private apply (crdt: PureCrdt<'state,'op>) (state: State<'state,'op>) =
    if Set.isEmpty state.Unstable
    then state.Stable
    else crdt.Apply(state.Stable, state.Unstable)
    
  let private terminateConnection (nodeId: ReplicaId) (state: State<'state,'op>) =
    match Map.tryFind nodeId state.Connections with
    | None -> state.Connections
    | Some conn ->
      conn.Timeout.Cancel()
      conn.Timer.Stop()
      Map.remove nodeId state.Connections
  
  let actorWith (crdt: PureCrdt<'state,'op>) (config: Config) (ctx: Actor<Protocol<'state,'op>>) =
    let rec active (crdt: PureCrdt<'state,'op>) (state: State<'state,'op>) (ctx: Actor<Protocol<'state,'op>>) = actor {
      match! ctx.Receive() with
      | Connect(nodeId, target) ->
        //target <! Replicate(state.Id, state.LatestVersion)
        Map.tryFind nodeId state.Connections |> Option.iter (fun c -> c.Timeout.Cancel())
        let policy = config.EvictionPolicy nodeId
        let timeout = ctx.Schedule config.DefaultReplicationTimeout ctx.Self (ReplicateTimeout nodeId)
        let status =
          { Timeout = timeout
            Endpoint = target
            Id = nodeId
            EvictionPolicy = policy
            Timer = Stopwatch.StartNew() }
        let state = { state with Connections = Map.add nodeId status state.Connections }
        return! active crdt state ctx
        
      | Query QueryMode.Latest ->
        let result = apply crdt state
        ctx.Sender() <! result
        return! active crdt state ctx
        
      | Query QueryMode.Stable ->
        ctx.Sender() <! state.Stable
        return! active crdt state ctx
        
      | Replicate(nodeId, filter) ->
        
        match Map.tryFind nodeId state.Evicted with
        | Some version when Version.compare filter version = Ord.Cc ->
          // inform node about it being evicted
          ctx.Sender() <! Evicted(nodeId, version)
          return! active crdt state ctx
          
        | _ ->
          // standard handle
          let conn = Map.find nodeId state.Connections
          match Version.compare filter state.StableVersion with
          | Ord.Lt -> conn.Endpoint <! Reset(state.Id, toSnapshot state) // send reset request if remote version is behind our stable
          | _ -> ()
          
          let unseen =
            state.Unstable
            |> Set.filter (fun op -> Version.compare op.Version filter > Ord.Eq) // get greater than or concurrent versions
          conn.Endpoint <! Replicated(state.Id, unseen)
          
          logDebugf ctx "received replicate request with timestamp %O. Sending %i ops." filter (Set.count unseen)
          return! active crdt state ctx
      
      | ReplicateTimeout nodeId ->
        let conn = Map.find nodeId state.Connections
        let elapsed = conn.Timer.Elapsed
        conn.Endpoint <! Replicate(state.Id, state.LatestVersion)
        let state = processEviction ctx state conn (TimedOut elapsed)
        return! active crdt state ctx
      
      | Reset(nodeId, snapshot) when Version.compare snapshot.StableVersion state.LatestVersion = Ord.Gt ->
        logDebugf ctx "reset state from %O: %O" nodeId snapshot.StableVersion 
        let state =
          { state with
              Stable = snapshot.Stable
              StableVersion = snapshot.StableVersion
              LatestVersion = Version.merge state.LatestVersion snapshot.StableVersion
              Observed = MTime.merge snapshot.Observed state.Observed
              Evicted = MTime.merge snapshot.Evicted state.Evicted }
        notify state (Stable(state.Stable, state.StableVersion))
        return! active crdt state ctx
          
      | Reset(nodeId, snapshot) ->
        logErrorf ctx "received Reset from %O with version: %O (local version: %O). Ignoring." nodeId snapshot.StableVersion state.LatestVersion
        return! active crdt state ctx
         
      | Replicated(nodeId, ops) when Set.isEmpty ops ->
        let state = refreshTimeout nodeId state ctx
        return! active crdt state ctx 
            
      | Replicated(nodeId, ops) ->
        let mutable state = state
        let evictedVersion = Map.tryFind nodeId state.Evicted |> Option.defaultValue Version.zero
        let actual =
          ops
          |> Set.filter (fun op ->
            Version.compare op.Version state.LatestVersion > Ord.Eq && // deduplicate
            not (op.Origin = nodeId && Version.compare op.Version evictedVersion = Ord.Cc)) // ignore events concurrent to eviction process
        
        for op in actual do
          let observed = state.Observed |> MTime.update nodeId op.Version
          // check if new incoming event is not obsolete
          let obsolete = state.Unstable |> Set.exists(fun o -> crdt.Obsoletes(o, op))
          // prune unstable operations, which have been obsoleted by incoming event
          let pruned = state.Unstable |> Set.filter (fun o -> not (crdt.Obsoletes(op, o)))
          let unstable =
            if obsolete then pruned
            else
              notify state (Emitted op)
              Set.add op pruned
          state <- { state with
                       Observed = observed
                       Unstable = unstable
                       LatestVersion = Version.merge op.Version state.LatestVersion }
        
        // check if some of unstable events have stabilized now  
        let state = stabilize ctx crdt state
        let state = refreshTimeout nodeId state ctx
        return! active crdt state ctx
      
      | Submit op ->
        let sender = ctx.Sender()
        let version = Version.inc state.Id state.LatestVersion
        let versioned =
          { Version = version
            Value = op
            Timestamp = DateTime.UtcNow
            Origin = state.Id }
        notify state (Emitted versioned)
        // prune unstable events, which have been obsoleted by this operation
        let pruned = state.Unstable |> Set.filter (fun o -> not (crdt.Obsoletes(versioned, o)))
        let observed = state.Observed |> MTime.update state.Id version
        let state = { state with Unstable = Set.add versioned pruned; LatestVersion = version; Observed = observed }
        
        // respond to a called with new state (not necessary but good for testing)
        let reply = apply crdt state
        logDebugf ctx "submitted operation %O at timestamp %O - state after application: %A" op version reply 
        sender <! reply
        return! active crdt state ctx

      | Evict nodeId ->
        let version = state.LatestVersion
        notify state (Notification.Evicted(nodeId, version))
        let msg = Evicted(nodeId, version)
        for e in state.Connections do
          e.Value.Endpoint <! msg
        let connections = terminateConnection nodeId state          
        let state = { state with
                        LatestVersion = version
                        Evicted = Map.add nodeId state.LatestVersion state.Evicted
                        Observed = Map.remove nodeId state.Observed |> Map.add state.Id version
                        Connections = connections }
        return! active crdt state ctx
      
      | Evicted(evicted, version) when evicted = state.Id ->
        notify state (Notification.Evicted(evicted, version))
        // evacuate - send all concurrent events to event stream
        for e in state.Unstable do
          if e.Origin = evicted && Version.compare e.Version version = Ord.Cc then
            notify state (Revoked e) 
        return Stop // stop current actor
      
      | Evicted(evicted, version) ->
        let connections = terminateConnection evicted state
        let (revoked, unstable) =
          state.Unstable
          |> Set.partition (fun o -> o.Origin = evicted && Version.compare o.Version version = Ord.Cc)
                    
        let state = { state with
                        Evicted = Map.add evicted version state.Evicted
                        Observed = Map.remove evicted state.Observed
                        Connections = connections
                        Unstable = unstable }
        
        notify state (Notification.Evicted(evicted, version))
        for e in revoked do
          notify state (Revoked e)
          
        return! active crdt state ctx
        
      | Subscribe subscriber ->
        monitorWith (Unsubscribe subscriber) ctx subscriber |> ignore
        let state = { state with Subscribers = Set.add subscriber state.Subscribers }
        subscriber <! Stable(state.Stable, state.StableVersion)
        return! active crdt state ctx
        
      | Unsubscribe subscriber ->
        let state = { state with Subscribers = Set.remove subscriber state.Subscribers }
        return! active crdt state ctx
    }
    
    let state =
      { Id = config.Id
        Stable = crdt.Default
        StableVersion = Version.zero
        LatestVersion = Version.zero
        Unstable = Set.empty
        Observed = Map.empty
        Evicted = Map.empty
        Subscribers = Set.empty
        Connections = Map.empty }
    active crdt state ctx
    
  let private replicateTimeout = TimeSpan.FromMilliseconds 200.
  let private defaultEvictionPolicy = fun _ _ -> Some replicateTimeout
  
  let actor crdt id ctx = actorWith crdt { Id = id; DefaultReplicationTimeout = replicateTimeout; EvictionPolicy = defaultEvictionPolicy } ctx