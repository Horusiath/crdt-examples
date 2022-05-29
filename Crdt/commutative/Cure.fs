/// The MIT License (MIT)
/// Copyright (c) 2022 Bartosz Sypytkowski

namespace Crdt.Commutative.Cure

open System
open Akka.Util
open Crdt
open Akkling

type Op = interface end
type Crdt =
    abstract Value: obj

type Message =
    | StartTxn
    | Commit
    | Abort
    | ReadKey of key:string * svc:VTime
    | ReadKeys of keys:Set<string>
    | Submit of operations: (string*Op) array
    | Prepare of Txn

and Endpoint = IActorRef<Message>

and Txn = IActorRef<Message>

[<RequireQualifiedAccess>]
module private Transaction =
    
    type State =
        { beginTimestamp: VTime
          partitions: Endpoint[]
          local: Map<string, Crdt>
          ops: Map<string, Op list> }
        
    let partitionByKey (partitions: Endpoint[]) (key: string) =
        partitions.[(MurmurHash.StringHash key) % partitions.Length]
        
    let rec receive (state: State) (ctx: Actor<Message>) = actor {
        let! msg = ctx.Receive()
        let sender = ctx.Sender ()
        let parent = ctx.Parent()
        match msg with
        | ReadKeys keys ->
            Async.Start (async {
                let! entries =
                    keys
                    |> Seq.map (fun key -> async {
                        let partition = partitionByKey state.partitions key
                        if partition = parent then
                            // key lives at the local partition
                            let crdt = Map.find key state.local
                            return (key, crdt.Value)
                        else
                            // key lives at remote partition
                            let value = partition <? ReadKey(key, state.beginTimestamp)
                            return (key, value)
                    })
                    |> Async.Parallel
                retype sender <! Map.ofArray entries
            })
            return Ignore
            
        | Submit ops ->
            return failwith "todo"
            
        | Commit when Map.isEmpty state.ops->
            // this was read-only transaction - no updates were made
            sender <! Ok (None)
            return Stop
            
        | Commit ->
            return failwith "todo"
            
        | Abort ->
            sender <! Ok (None)
            return Stop
            
        | _ -> return Unhandled
    }
    
    let props (partitions: Endpoint[]) (beginTimestamp: VTime) (local: Map<string, Crdt>) : Props<Message> =
        let state =
            { beginTimestamp = beginTimestamp
              partitions = partitions
              local = local
              ops = Map.empty }
        Props.props (receive state)

[<RequireQualifiedAccess>]
module private Partition =
    
    type State =
        { id: ReplicaId
          partitions: Endpoint[]
          version: VTime
          stable: Map<string, Crdt> }
        
    let rec receive (state: State) (ctx: Actor<Message>) = actor {
        let! msg = ctx.Receive()
        let sender = ctx.Sender ()
        match msg with
        | StartTxn ->
            let txn = spawnAnonymous ctx (Transaction.props state.partitions state.version state.stable)
            sender <! txn
            return Ignore
        | Commit -> return failwith "todo"
        | Abort -> return failwith "todo"
        | ReadKeys(keys) -> return failwith "todo"
        | Submit(ops) -> return failwith "todo"
        | Prepare txn -> return failwith "todo"
    }
    
    let props (partitions: Endpoint[]) (id: int) : Props<Message> = failwith "todo"
        

[<RequireQualifiedAccess>]
module Cure =
    
    /// Creates a new `Partition` actor props.
    let props (partitions: Endpoint[]) (id: int) : Props<Message> = Partition.props partitions id
    
    /// Starts new read-write transaction.
    let beginTxn (partition: Endpoint) : Async<Txn> = partition <? StartTxn
    
    /// Commits existing `transaction` at the given `partition`.
    let commit (transaction: Txn) : Async<unit> = transaction <? Commit
    
    /// Aborts existing `transaction` at the given `partition`, reverting all changes made within it.
    let abort (transaction: Txn) : Async<unit> = transaction <? Abort
    
    /// Reads entries associated with given `keys`.
    let read (transaction: Txn) (keys: #seq<string>) : Async<Map<string, obj>> =
        transaction <? ReadKeys(Set.ofSeq keys)
    
    /// Creates or updates `entries` with a new values withing a scope of a given `transaction`. 
    let submit (transaction: Txn) (ops: #seq<(string * Op)>) : Async<unit> =
        transaction <? Submit(Array.ofSeq ops) |> Async.Ignore