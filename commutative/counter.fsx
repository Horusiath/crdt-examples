/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "common.fsx"
#load "protocol.fsx"

open Crdt.Protocol

[<RequireQualifiedAccess>]
module Counter =
    
    let private crdt =
        { new Crdt<int64,int64,int64,int64> with
            member _.Default = 0L
            member _.Query crdt = crdt
            member _.Prepare(_, op) = op
            member _.Effect(counter, e) = counter + e.Data }
    
    /// Used to create replication endpoint handling operation-based Counter protocol.
    let props db replica ctx = replicator crdt db replica ctx
    
    /// Increment counter maintainer by given `ref` endpoint by a given delta (can be negative).
    let inc (by: int64) (ref: Endpoint<int64,int64,int64>) : Async<int64> = ref <? Command by
    
    /// Retrieve the current state of the counter maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<int64,int64,int64>) : Async<int64> = ref <? Query