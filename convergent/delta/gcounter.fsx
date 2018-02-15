/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../../common.fsx"

type GCounter = GCounter of values:Map<ReplicaId, int64> * delta:GCounter option

/// A delta-state implementation of immutable grow-only counter
[<RequireQualifiedAccess>]
module GCounter =
    
    /// Creates an empty G-Counter.
    let zero = GCounter(Map.empty, None)

    /// Returns a value of provided G-Counter.
    let value (GCounter(v, _)) =
        v |> Map.toSeq |> Seq.map snd |> Seq.fold (+) 0L

    /// Increments a G-Counter using replica `r` tag.
    let inc r (GCounter(v, d)) =
        let add1 map = Helpers.upsert r 1L ((+) 1L) map
        let (GCounter(dmap,None)) = defaultArg d zero
        GCounter(add1 v, Some(GCounter(add1 dmap, None)))

    /// Merges two G-Counter instances, returning another G-Counter as results of their convergence.
    let rec merge (GCounter(a, da)) (GCounter(b, db)) = 
        let values = a |> Map.fold (fun acc k va -> Helpers.upsert k va (max va) acc) b
        let delta = Helpers.mergeOption merge da db
        GCounter(values, delta)
        
    /// Merges a G-Counter delta with G-Counter instance, returning new G-Counter instance.
    let mergeDelta delta counter = merge counter delta
    
    /// Splits a given G-Counter into G-Counter with prunned delta and its delta, if provided.
    let split (GCounter(v, d)) = GCounter(v, None), d
