/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"
#load "pncounter.fsx"

/// A bounded counter, which enables to perform  Counter-like increment/decrement
/// operation. Unlike `GCounter`/`PNCounter` it's allowed to have a max boundary (hence
/// name), above which any increments will fail to execute.
type BCounter = BCounter of PNCounter * Map<(ReplicaId*ReplicaId), int64>

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
            if   src = rep then acc - v
            elif dst = rep then acc + v
            else acc) (PNCounter.value counter)

    /// Returns a value of the counter.
    let value (BCounter(c, _)) = PNCounter.value c

    /// Increments the counter value on a local replica. It returns updated counter,
    /// because this implementation allways allows for incrementing values.
    let inc rep value (BCounter(c, others)) =
        BCounter(PNCounter.inc rep value c, others)

    /// Decrements the counter value on a local replica. If there's no quota left,
    /// it will fail with Error(n), where `n` is the current quota allowed for a given replica.
    let dec rep value counter =
        let q = quota rep counter
        if q < value then Error q 
        else 
            let (BCounter(c, others)) = counter
            Ok <| BCounter(PNCounter.dec rep value c, others)

    /// Moves some amount of quota from one counter to another one. If there's not
    /// enough quota to satisfy transfer, it will fail with Error(n), where `n` is the current
    /// quota allowed for a given source replica.
    let move src dst value counter =
        let q = quota src counter
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
        
    [<Struct>]
    type Merge = 
        interface IConvergent<BCounter> with
            member __.merge a b = merge a b 