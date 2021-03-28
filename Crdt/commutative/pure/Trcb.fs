/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace  Crdt.Commutative.Pure
//[<AutoOpen>]
//module Crdt.Commutative.Pure.Protocol
//
//open System
//open Akka.Actor
//open Akkling
//open Crdt
//open FSharp.Control
//
//type Endpoint<'state,'op> = IActorRef<Protocol<'state,'op>>
//and Protocol<'state,'op> =
//    /// Attaches another replica to continuously synchronize with current one.
//    | Connect of ReplicaId * Endpoint<'state,'op>
//    /// Request to read up to `maxCount` events from a given replica starting from `seqNr`. Additionally a `filter`
//    /// is provided to deduplicate possible events on the sender side (it will be then used second time on receiver
//    /// side). This message is expected to be replied with `Recovered` series, optionally preceeded by `Reset`
//    /// (if a snapshot needs to be provided first) which contains all events satisfying seqNr/filter criteria. 
//    | Replicate of seqNr:uint64 * maxCount:int * filter:VTime * replyTo:Endpoint<'state,'op>
//    /// Resets current replicator state to a given one.
//    | Reset of from:ReplicaId * snapshot:Snapshot<'state>
//    /// Replicated operation passed locally or from remote connection. 
//    | Replicated of from:ReplicaId * toSeqNr:uint64 * operations:Op<'op>[]
//    /// Timeout for `Replicate` request.
//    | ReplicateTimeout of ReplicaId
//    /// Read current replica state.
//    | Query
//    /// Submit current replica operations.
//    | Submit of 'op 
//
//module Replicator =
//    
//    let recoverTimeout = TimeSpan.FromMilliseconds 200.
//    
//    type State<'state,'op> =
//        { /// Snapshot of the last stable operation.
//          Stable: Snapshot<'state>
//          Latest: 'state
//          /// An observed space of all received versions from all replicas. Can be used to determine the latest - version
//          /// at the current local replica ID - and the stable versions.
//          Observed: MTime
//          /// All received sequence numbers, including local one.
//          SeqNrs: Map<ReplicaId, uint64>
//          /// Collection of unstable operations, which are greater than or concurrent to `Stable` snapshot.
//          Unstable: Set<Op<'op>>
//          /// Active connections.
//          Connections: Map<ReplicaId, struct(Endpoint<'state,'op> * ICancelable)> }
//        
//    [<RequireQualifiedAccess>]
//    module State =
//            
//        /// Checks if current state is stable.
//        let isStable state = state.Unstable |> Set.isEmpty
//        
//        /// Returns a sequence number for a given replica at observed snapshot.
//        let seqNr (nodeId: ReplicaId) (state: State<_,_>) : uint64 =
//            Map.tryFind nodeId state.SeqNrs |> Option.defaultValue 0UL
//            
//        /// Returns a vector timestamp for a given replica at observed snapshot.
//        let version (nodeId: ReplicaId) (state: State<_,_>) : VTime =
//            Map.tryFind nodeId state.Observed |> Option.defaultValue Version.zero
//            
//        /// Gets new series of timestamps for a given replica.
//        let next (replica: ReplicaId) (state: State<_,_>) =
//            let seqNr = seqNr replica state + 1UL
//            let timestamp = version replica state |> Version.inc replica
//            let state =
//                { state with
//                    Observed = Map.add replica timestamp state.Observed
//                    SeqNrs = Map.add replica seqNr state.SeqNrs }
//            (seqNr, timestamp, state)
//                    
//        let unseen local remote (state: State<_,'op>) (op: Op<'op>) =
//            if seqNr remote state >= op.OriginSeqNr then false
//            else Version.compare op.Version (version local state) > Ord.Eq
//            
//        let stabilize (state: State<'state, 'op>) =
//            let stableTimestamp = state.Observed |> MTime.min
//            let stable, unstable =
//                state.Unstable
//                |> Set.partition (fun op -> Version.compare op.Version stableTimestamp <= Ord.Eq)
//            (stable, unstable, stableTimestamp)
//                    
//    let actor (crdt: PureCrdt<'state,'op>) (db: Db) (id: ReplicaId) (ctx: Actor<Protocol<_,_>>) =
//        let rec active (state: State<'state,'op>) (db: Db) (id: ReplicaId) (ctx: Actor<Protocol<_,_>>) = actor {
//            match! ctx.Receive() with
//            | Query ->
//                ctx.Sender() <! state.Latest
//                return! active state db id ctx
//                
//            | Connect(nodeId, ref) ->
//                // connect with the remote replica, and start synchronizing with it
//                let seqNr = State.seqNr nodeId state
//                let version = State.version nodeId state
//                ref <! Replicate(seqNr+1UL, 100, version, ctx.Self)
//                logDebugf ctx "connected with replica %s. Sending read request starting from %i" nodeId (seqNr+1UL)
//                state.Connections |> Map.tryFind nodeId |> Option.iter (fun struct(_, timeout) -> timeout.Cancel())
//                let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout nodeId)
//                let state = { state with Connections = Map.add nodeId struct(ref, timeout) state.Connections } 
//                return! active state db id ctx
//                
//            | Replicate(seqNr, maxCount, filter, replyTo) ->
//                let version = State.version id state
//                match Version.compare version filter with
//                | Ord.Gt -> replyTo <! Reset(id, state.Stable) // our state is strictly greater, send it
//                | Ord.Lt | Ord.Cc -> replyTo <! Replicate(State.seqNr id state, 100, version, ctx.Self) // our state is missing some updates, ask for them
//                | _ -> ()
//                
//                let cursor = db.GetOps seqNr
//                let! (seqNr, batch) = async {
//                    let arr = ResizeArray()
//                    use e = cursor.GetEnumerator()
//                    let mutable i = 0
//                    let mutable seqNr = seqNr
//                    while i < maxCount do
//                        match! e.MoveNext() with
//                        | Some op ->
//                            i <- i + 1
//                            seqNr <- op.LocalSeqNr
//                            if Version.compare op.Version version > Ord.Eq then arr.Add op
//                        | None -> i <- maxCount
//                    return (seqNr, arr.ToArray())
//                }
//                replyTo <! Replicated(id, seqNr, batch)                
//                return! active state db id ctx 
//            
//            | Reset(from, snapshot) ->
//                if Version.compare snapshot.Version state.Stable.Version = Ord.Gt then
//                    let state =
//                        { state with
//                            Stable = snapshot
//                            Unstable = state.Unstable |> Set.filter (fun o -> not(crdt.Obsolete(snapshot, o)))
//                            Observed = MTime.update from snapshot.Version state.Observed
//                            SeqNrs = Version.set from snapshot.SeqNr state.SeqNrs }
//                    return! active state db id ctx
//                else
//                    logErrorf ctx "Incoming reset request version %O vs. local stable %O" snapshot.Version state.Stable.Version
//                    return! active state db id ctx
//                
//            | Replicated(from, toSeqNr, operations) ->
//                let mutable nstate = state
//                let mutable remoteSeqNr = State.seqNr from nstate
//                let mutable localSeqNr = State.seqNr id nstate
//                let mutable localVersion = State.version id nstate
//                let toSave = ResizeArray()
//                // for all events not seen by the current node, rewrite them to use local sequence nr, update the state
//                // and save them in the database
//                for op in operations |> Array.filter (State.unseen id from state) do
//                    logDebugf ctx "replicating event %O from replica %s" op from
//                    localVersion <- Version.merge localVersion op.Version
//                    remoteSeqNr <- Math.Max(remoteSeqNr, op.LocalSeqNr) // increment observed remote sequence nr
//                    localSeqNr <- localSeqNr + 1UL
//                    let op = { op with LocalSeqNr = localSeqNr }
//                    let obsolete = crdt.Obsolete(nstate.Stable, op) || nstate.Unstable |> Set.exists (fun o -> crdt.Obsolete(o, op))
//                    nstate <- { nstate with
//                                    Latest = if obsolete then nstate.Latest else crdt.Apply(nstate.Latest, op)
//                                    Unstable = if obsolete then nstate.Unstable else Set.add op nstate.Unstable
//                                    SeqNrs = Map.add from remoteSeqNr nstate.SeqNrs
//                                    Observed = MTime.update id op.Version nstate.Observed }
//                    toSave.Add (Op op)
//                    
//                let stable, unstable, stableTimestamp = State.stabilize nstate
//                if not (Set.isEmpty stable) then
//                    logDebugf ctx "stabilized operations at timestamp %O: %A" stableTimestamp (stable |> Set.map (fun op -> op.Value))
//                    let mutable snapshot = nstate.Stable
//                    for op in stable do
//                        snapshot <- { snapshot with Value = crdt.Apply(snapshot.Value, op) }
//                    nstate <- { nstate with
//                                  Unstable = unstable
//                                  Stable = { snapshot with Version = stableTimestamp } }
//                    toSave.Add (Snapshot nstate.Stable)
//                
//                do! db.PutBatch toSave // save all unseen events together with updated state
//                do! db.Prune(0UL, stableTimestamp)
//                                
//                let struct(target, cancel) = Map.find from state.Connections
//                cancel.Cancel()
//                let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout from)
//                let nstate = { nstate with Connections = Map.add from (struct(target, timeout)) nstate.Connections }
//                if not (Array.isEmpty operations) then
//                    target <! Replicate(toSeqNr+1UL, 100, localVersion, ctx.Self) // continue syncing
//                return! active nstate db id ctx 
//            
//            | ReplicateTimeout nodeId ->
//                logDebugf ctx "%s didn't replied to read request in time" nodeId
//                let version = State.version id state
//                let seqNr = State.seqNr nodeId state
//                let struct(ref, cancel) = Map.tryFind nodeId state.Connections |> Option.defaultWith (fun () -> failwithf "didn't find '%s' among replicating nodes" nodeId)
//                ref <! Replicate(seqNr+1UL, 100, version, ctx.Self)
//                cancel.Cancel()
//                let timeout = ctx.Schedule recoverTimeout ctx.Self (ReplicateTimeout nodeId)
//                let state = { state with Connections = Map.add nodeId (struct(ref, timeout)) state.Connections }
//                return! active state db id ctx
//            
//            | Submit action ->
//                let sender = ctx.Sender()
//                let (seqNr, version, state) = State.next id state
//                let op: Op<'op> =
//                    { Origin = id
//                      Timestamp = DateTime.UtcNow
//                      OriginSeqNr = seqNr
//                      LocalSeqNr = seqNr
//                      Version = version
//                      Value = action }
//                do! db.PutBatch [Op op]
//                let latest = crdt.Apply(state.Latest, op)
//                sender <! latest
//                return! active { state with Latest = latest } db id ctx }
//        
//        and recovering (state: State<'state,'op>) (ops: Set<Op<'op>>) (db: Db) (from: ReplicaId) (ctx: Actor<Protocol<_,_>>) = actor {
//            match! ctx.Receive() with
//            | Reset(nodeId, snapshot) when nodeId = from ->
//                let state = { state with
//                                Observed = Map.add id snapshot.Version Map.empty
//                                Stable = snapshot }
//                return! recovering state ops db from ctx
//            | Replicated(nodeId, _, o) when nodeId = from ->
//                // during recovery we don't need to do anything with incoming ops 
//                let ops = ops + (Set.ofArray o)
//                return! recovering state ops db from ctx
//            | ReplicateTimeout nodeId when nodeId = from ->
//                ctx.UnstashAll()
//                return! active state db id ctx
//            | _ -> // stash all other messages until recovery is complete
//                ctx.Stash()
//                return! recovering state ops db from ctx
//        }
//        
//        async {            
//            // apply all events that happened since snapshot has been made
//            let ops = ResizeArray<Op<'op>>()
//            let! snapshot = db.GetSnapshot()
//            let seqNr =
//                match snapshot with
//                | None -> 0UL
//                | Some s ->
//                    ctx.Self <! Reset(id, s)
//                    s.SeqNr
//            for op in db.GetOps (seqNr + 1UL) do
//                ops.Add op
//                let count = ops.Count
//                if count = 100 then
//                    ctx.Self <! Replicated(id, ops.[count-1].LocalSeqNr, ops.ToArray())
//                    ops.Clear()
//            if ops.Count <> 0 then
//                ctx.Self <! Replicated(id, ops.[ops.Count-1].LocalSeqNr, ops.ToArray())
//            ctx.Self <! ReplicateTimeout id
//        } |> Async.Start
//        
//        // we start by recovering from local state
//        let snapshot: State<'state,'op> =
//            { Stable = { Value = crdt.Default; Version = Version.zero; SeqNr = 0UL; Timestamp = DateTime.MinValue }
//              Latest = crdt.Default
//              Observed = Map.empty
//              SeqNrs = Map.empty
//              Unstable = Set.empty
//              Connections = Map.empty }
//        recovering snapshot Set.empty db id ctx