/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Commutative

open System
open Crdt
open Akkling

[<RequireQualifiedAccess>]    
module LWWRegister =
        
    [<Struct>]
    type LWWRegister<'a> =
        { Timestamp: struct(DateTime * ReplicaId)
          Value: 'a voption }
        
    type Operation<'a> = DateTime * 'a
    
    type Endpoint<'a> = Endpoint<LWWRegister<'a>, 'a, Operation<'a>>
    
    let private crdt : Crdt<LWWRegister<'a>, 'a voption, 'a, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default = { Timestamp = struct(DateTime.MinValue, ""); Value = ValueNone }
            member _.Query crdt = crdt.Value
            member _.Prepare(_, value) = (DateTime.UtcNow, value)
            member _.Effect(existing, e) =
                let (at, value) = e.Data
                let timestamp = struct(at, e.Origin)
                if existing.Timestamp < timestamp then
                    { existing with Timestamp = timestamp; Value = ValueSome value }
                else existing }
    
    let props db replica ctx = replicator crdt db replica ctx
    let update (value: 'a) (ref: Endpoint<'a>) : Async<'a voption> = ref <? Command value
    let query (ref: Endpoint<'a>) : Async<'a voption> = ref <? Query