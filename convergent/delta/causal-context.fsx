/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../../common.fsx"

[<RequireQualifiedAccess>]
module Causal =

    type Dot = ReplicaId * int64
    type DotStore = {
        CausalContext: Map<ReplicaId, int64>
        Dots: Set<Dot>
    }
    
    let max c = 
        if c |> Map.isEmpty 
        then 0L 
        else c |> Map.toSeq |> Seq.map snd |> Seq.max

    //TODO: continue implementation