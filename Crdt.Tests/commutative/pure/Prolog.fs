[<AutoOpen>]
module Crdt.Tests.Commutative.Pure.Prolog

open FSharp.Control
open Crdt
open Crdt.Commutative.Pure

let wait = Async.RunSynchronously

type InMemoryDb(id: ReplicaId) =
    let snapshot: obj option ref = ref None
    let mutable operations : ResizeArray<(VTime * obj)> = ResizeArray()
    interface Db with
        member this.GetOperations<'op>(filter: VTime) : AsyncSeq<Versioned<'op>> =
            let sorted =
                operations
                |> Seq.map snd
                |> Seq.cast<Versioned<'op>>
                |> Seq.sort
            AsyncSeq.ofSeq sorted
        member this.GetSnapshot() : Async<Snapshot<'state> option> = async {
            match !snapshot with
            | Some state -> return Some (state :?> Snapshot<'state>)
            | _ -> return None
        }
        member this.Prune(stable: VTime) = async {
            operations.RemoveAll(System.Predicate<VTime * obj>(fun (t, _) -> Version.compare t stable = Ord.Lt )) |> ignore
        }
        member this.Store(entries) = async {
            for entry in entries do
                match entry with
                | Op versioned ->
                    operations.Add (versioned.Version, upcast versioned)
                | Snapshot snap ->
                    snapshot := Some (upcast snap)
        }