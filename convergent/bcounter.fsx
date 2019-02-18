/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"
#load "pncounter.fsx"

/// A bounded counter, which enables to perform  Counter-like increment/decrement
/// operation. Unlike `GCounter`/`PNCounter` it's allowed to have a max boundary (hence
/// name), above which any increments will fail to execute.
type BCounter = BCounter of Map<ReplicaId, int64>

/// Result of trying to apply increment/decrement operation
/// over `BCounter`. Can be either:
/// 
/// - `Ok` once counter value could be successfully incremented/decremented.
/// - `Fail` when operation is known to fail due to counter boundaries reached.
/// - `Inconclusive` when there was no sufficient information to determine one of 
/// previous two results.
/// 
/// In case of `Inconclusive` operation, it means that local replica has not enough 
/// quota, but it's known that sum of quotas on remote replicas is enough to satisfy
/// that operation. In that case you need to ask a remote replica(s) to transfer their 
/// quota first to local replica, and then try to execute operation again.
type OperationResult =
    | Ok of BCounter
    | Inconclusive 
    | Fail

[<RequireQualifiedAccess>]
module BCounter =

    /// A default/empty instance of a `BCounter`.
    let zero = BCounter Map.empty

    /// Increases the max quota of a local replica, effectivelly increasing total `BCounter` boundary.
    let fill rep quota = ???

    /// Increments the counter value on a local replica.
    let inc rep value = ???


