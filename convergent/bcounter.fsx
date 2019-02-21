/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt
open System.Web.UI.WebControls

#load "../common.fsx"
#load "pncounter.fsx"

/// A bounded counter, which enables to perform  Counter-like increment/decrement
/// operation. Unlike `GCounter`/`PNCounter` it's allowed to have a max boundary (hence
/// name), above which any increments will fail to execute.
type BCounter = BCounter of PNCounter * Map<(ReplicaId*ReplicaId), int64>

/// Result of trying to apply increment/decrement operation
/// over `BCounter` can be either:
/// 
/// - `Ok` once counter value could be successfully incremented/decremented.
/// - `Error` when operation is known to fail due to counter boundaries reached, 
/// or when there was no sufficient information to determine one of previous two results.
/// 
/// In case of inconclusive error operation, it means that local replica has not enough 
/// quota, but it's known that sum of quotas on remote replicas is enough to satisfy
/// that operation. In that case you need to ask a remote replica(s) to transfer their 
/// quota first to local replica, and then try to execute operation again.
[<RequireQualifiedAccess>]
module BCounter =

    /// A default/empty instance of a `BCounter`.
    let zero = BCounter(PNCounter.zero, Map.empty)

    /// Returns a quota of the `counter`, that current replica owns. Computed as:
    /// 
    ///     value(pncounter) + quota received - quota given
    /// 
    let quota rep (BCounter(counter, others)) =
        others
        |> Map.fold (fun acc (src, dst) v ->
            if   src = rep then acc + v
            elif dst = rep then acc - v
            else acc) (PNCounter.value counter)

    /// Returns a value of the counter.
    let value (BCounter(c, _)) = PNCounter.value c

    /// Increments the counter value on a local replica.
    let inc rep value (BCounter(c, others)) =
        BCounter(PNCounter.inc rep c, others)

    /// Decrements the counter value on a local replica.
    let dec rep value counter =
        let q = quota rep counter
        if q < 0L then Error q 
        else 
            let (BCounter(c, others)) = counter
            Ok <| BCounter(PNCounter.dec rep c, others)

    /// Moves some amount of quota from one counter to another one.
    let move src dst value counter =
        let q =  quota src counter
        if value > q then Error q
        else
            let (BCounter(c, others)) = counter
            let others' =
                match Map.tryFind (src,dst) others with
                | Some v -> Map.add (src,dst) (v+value) others
                | None -> Map.add (src,dst) value others
            Ok (BCounter(c, others'))

    /// Merges two instances of a given BCounter. 
    let merge (BCounter(ax, mx)) (BCounter(ay, my)) =
        let az = PNCounter.merge ax ay
        let mz =my |> Map.fold (fun acc k v2 -> Helpers.upsert k v2 (max v2) acc) mx
        BCounter(az, mz)