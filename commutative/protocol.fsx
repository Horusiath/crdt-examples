module Crdt.Protocol

#r "nuget: Akkling"
#load "./common.fsx"

open Akka.Actor
open Akkling
open System
open FSharp.Control

type Endpoint<'s,'c,'e> = IActorRef<Protocol<'s,'c,'e>>

and Protocol<'s,'c,'e> =
    /// Attaches another replica to continuously synchronize with current one.
    | Connect of replicaId:ReplicaId * Endpoint<'s,'c,'e>
    /// Request to read up to `maxCount` events from a given replica starting from `seqNr`. Additionally a `filter`
    /// is provided to deduplicate possible events on the sender side (it will be then used second time on receiver side).
    /// This message is expected to be replied with `Recovered`, which contains all events satisfying seqNr/filter criteria. 
    | Replicate of seqNr:uint64 * maxCount:int * filter:VTime * replyTo:Endpoint<'s,'c,'e>
    | ReplicateTimeout of ReplicaId
    /// Response to `Recover` - must always be send. Empty content notifies about end of event stream. `toSeqNr` informs
    /// up to which sequence number this message advanced.
    | Replicated of from:ReplicaId * toSeqNr:uint64 * events:Event<'e>[]
    /// Request for a state. It should be replied with state being application of `Crdt.Query` over `ReplicationState.Crdt`.
    | Query
    /// Persists an event into current replica. Replied with updated, materialized state after success. 
    | Command of 'c
    /// Message send at the beginning of recovery phase with the latest persisted snapshot of the state (if there was any) 
    | Loaded of ReplicationState<'s>
    /// Periodic trigger to persist current state snapshot (only performed if state has changed since last snapshot, tracked by IsDirty flag). 
    | Snapshot
        
and ReplicationState<'s> =
    { /// Unique identifier of a given replica/node.
      Id: ReplicaId
      /// Checks if replication state has been modified after being persisted.
      IsDirty: bool
      /// Counter used to assign unique sequence number for the events to be stored locally.
      SeqNr: uint64
      /// Version vector describing the last observed event.
      Version: VTime
      /// Sequence numbers of remote replicas. When synchronizing (via `Recover` message) with remote replicas,
      /// we start doing so from the last known sequence numbers we received.
      Observed: Map<ReplicaId, uint64>
      /// CRDT object that is replicated.
      Crdt: 's }
    
[<RequireQualifiedAccess>]
module ReplicationState =
    let inline create (id: ReplicaId) state = { Id = id; IsDirty = false; SeqNr = 0UL; Version = Map.empty; Observed = Map.empty; Crdt = state }
    
    /// Checks if current event has NOT been observed by a replica identified by state. Unseen events are those, which
    /// have SeqNr higher than the highest observed sequence number on a given node AND their version vectors were not
    /// observed (meaning they are either greater or concurrent to current node version). 
    let unseen nodeId (state: ReplicationState<'s>) (e: Event<'e>) =
        match Map.tryFind nodeId state.Observed with
        | Some ver when e.OriginSeqNr <= ver -> false
        | _ -> (Version.compare e.Version state.Version) > Ord.Eq
        
[<Interface>]           
type Db =
    abstract SaveSnapshot: 's -> Async<unit>
    abstract LoadSnapshot: unit -> Async<'s option>
    abstract LoadEvents: startSeqNr:uint64 -> AsyncSeq<Event<'e>>
    abstract SaveEvents: events:Event<'e> seq -> Async<unit>

/// Use database `cursor` to read up to `count` elements and send them to the `target` as Recovered message.
/// Send only entries that have keys starting with a given `prefix` (eg. events belonging to specific nodeId).
/// Use `filter` to skip events that have been seen by the `target`. 
let replay (nodeId: ReplicaId) (filter: VTime) (target: Endpoint<'s,'c,'e>) (events: AsyncSeq<Event<'e>>) (count:int) = async {
    let buf = ResizeArray()
    let mutable cont = count > 0
    let mutable i = 0
    let mutable lastSeqNr = 0UL
    use cursor = events.GetEnumerator()
    while cont do
        match! cursor.MoveNext() with
        | Some e ->
            if Version.compare e.Version filter > Ord.Eq then
                buf.Add(e)
                i <- i + 1
            cont <- i < count
            lastSeqNr <- Math.Max(lastSeqNr, e.LocalSeqNr)
        | _ -> cont <- false
    let events = buf.ToArray()
    target <! Replicated(nodeId, lastSeqNr, events)
}
    
let recoverTimeout = TimeSpan.FromSeconds 5.
                   
type ReplicationStatus<'s,'c,'e> =
    { /// Access point for the remote replica.
      Endpoint: Endpoint<'s,'c,'e>
      /// Cancellation token for pending `RecoverTimeout`.
      Timeout: ICancelable }
                   
let replicator (crdt: Crdt<'crdt,'state,'cmd,'event>) (db: Db) (id: ReplicaId) (ctx: Actor<Protocol<_,_,_>>) =    
    /// Cancel last pending `RecoverTimeout` task, and schedule it again.
    let refreshTimeouts nodeId progresses (ctx: Actor<_>) =
        let p = Map.find nodeId progresses
        p.Timeout.Cancel()
        let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout nodeId)
        Map.add nodeId { p with Timeout = timeout } progresses
    
    let rec active (db: Db) (state: ReplicationState<'crdt>) (replicatingNodes: Map<ReplicaId, ReplicationStatus<'crdt,'cmd,'event>>) (ctx: Actor<_>) = actor {
        match! ctx.Receive() with
        | Query ->
            ctx.Sender() <! crdt.Query state.Crdt
            return! active db state replicatingNodes ctx
            
        | Replicate(from, count, filter, sender) ->
            logDebugf ctx "received recover request from %s: seqNr=%i, vt=%O" sender.Path.Name from filter
            let cursor = db.LoadEvents(from)
            replay state.Id filter sender cursor count |> Async.Start
            return! active db state replicatingNodes ctx 
            
        | Replicated(nodeId, lastSeqNr, [||]) ->
            // if we received empty event list, this node is up to date with `nodeId`
            // just schedule timeout, so when it happens we ask to Recover again
            logDebugf ctx "%s reached end of updates" nodeId
            let prog = refreshTimeouts nodeId replicatingNodes ctx
            let observedSeqNr = Map.tryFind nodeId state.Observed |> Option.defaultValue 0UL
            if lastSeqNr > observedSeqNr then
                let nstate = { state with Observed = Map.add nodeId lastSeqNr state.Observed }
                do! db.SaveSnapshot(nstate.Id, nstate)
                return! active db nstate prog ctx
            else
                return! active db state prog ctx
            
        | Replicated(nodeId, lastSeqNr, events) ->
            let mutable nstate = state
            let mutable remoteSeqNr = Map.tryFind nodeId nstate.Observed |> Option.defaultValue 0UL
            let toSave = ResizeArray()
            // for all events not seen by the current node, rewrite them to use local sequence nr, update the state
            // and save them in the database
            for e in events |> Array.filter (ReplicationState.unseen nodeId state) do
                logDebugf ctx "replicating event %O from replica %s" e nodeId
                let seqNr = nstate.SeqNr + 1UL
                let version = Version.merge nstate.Version e.Version // update current node version vector
                remoteSeqNr <- Math.Max(remoteSeqNr, e.LocalSeqNr) // increment observed remote sequence nr
                let nevent = { e with LocalSeqNr = seqNr }
                nstate <- { nstate with
                              Crdt = crdt.Effect(nstate.Crdt, nevent)
                              SeqNr = seqNr
                              Version = version
                              Observed = Map.add nodeId remoteSeqNr nstate.Observed }
                toSave.Add nevent
            do! db.SaveEvents toSave // save all unseen events together with updated state
            //do! db.SaveSnapshot nstate // in practice snapshot should be applied on condition (ideally in the same transaction)
            let target = Map.find nodeId replicatingNodes
            target.Endpoint <! Replicate(lastSeqNr+1UL, 100, nstate.Version, ctx.Self) // continue syncing 
            let prog = refreshTimeouts nodeId replicatingNodes ctx
            return! active db { nstate with IsDirty = true } prog ctx
            
        | ReplicateTimeout nodeId ->
            // if we didn't received Recovered in time or the last one was empty, upon timeout just retry the request
            logDebugf ctx "%s didn't replied to read request in time" nodeId
            let seqNr = Map.tryFind nodeId state.Observed |> Option.defaultValue 0UL
            let p = Map.find nodeId replicatingNodes
            p.Endpoint <! Replicate(seqNr+1UL, 100, state.Version, ctx.Self) 
            let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout nodeId)
            let prog = Map.add nodeId { p with Timeout = timeout } replicatingNodes
            return! active db state prog ctx
            
        | Command(cmd) ->
            let sender = ctx.Sender()
            let seqNr = state.SeqNr + 1UL
            let version = Version.inc state.Id state.Version
            let data = crdt.Prepare(state.Crdt, cmd) // handle the command, produce event
            let event = { Origin = state.Id; OriginSeqNr = seqNr; LocalSeqNr = seqNr; Version = version; Data = data }
            let ncrdt = crdt.Effect(state.Crdt, event) // update the state with produced event
            let nstate = { state with Version = version; SeqNr = seqNr; Crdt = ncrdt }
            // store new event atomically with updated state
            do! db.SaveEvents [event]
            logDebugf ctx "stored event %O in a database" event
            sender <! crdt.Query ncrdt // send updated materialized CRDT state back to the sender
            return! active db { nstate with IsDirty = true } replicatingNodes ctx
                        
        | Connect(nodeId, endpoint) ->
            // connect with the remote replica, and start synchronizing with it
            let seqNr = Map.tryFind nodeId state.Observed |> Option.defaultValue 0UL
            endpoint <! Replicate(seqNr+1UL, 100, state.Version, ctx.Self)
            logDebugf ctx "connected with replica %s. Sending read request starting from %i" nodeId (seqNr+1UL)
            let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout nodeId)
            return! active db state (Map.add nodeId { Endpoint = endpoint; Timeout = timeout } replicatingNodes) ctx
            
        | Snapshot when state.IsDirty ->
            logDebugf ctx "Snapshot triggered"
            let nstate = { state with IsDirty = false }
            do! db.SaveSnapshot nstate
            return! active db nstate replicatingNodes ctx
            
        | _ -> return Unhandled
    }
    
    let rec recovering (db: Db) (ctx: Actor<_>) = actor {
        match! ctx.Receive() with
        | Loaded state ->
            logDebugf ctx "Recovery phase done with state: %O" state
            ctx.UnstashAll()
            let interval = TimeSpan.FromSeconds 5.
            ctx.ScheduleRepeatedly interval interval ctx.Self Snapshot |> ignore
            return! active db state Map.empty ctx
            
        | _ ->
            // stash all other operations until recovery is complete
            ctx.Stash()
            return! recovering db ctx
    }
    
    async {
        // load state from DB snapshot or create a new empty one
        let! snapshot = db.LoadSnapshot()
        let mutable state = snapshot |> Option.defaultValue (ReplicationState.create id crdt.Default)
        // apply all events that happened since snapshot has been made
        for event in db.LoadEvents (state.SeqNr + 1UL) do
            state <- { state with
                          Crdt = crdt.Effect(state.Crdt, event)
                          SeqNr = event.LocalSeqNr
                          Version = Version.merge event.Version state.Version
                          Observed = Map.add event.Origin event.OriginSeqNr state.Observed }
        
        ctx.Self <! Loaded state
    } |> Async.Start
    
    recovering db ctx