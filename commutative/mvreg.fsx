/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "common.fsx"
#load "protocol.fsx"

open System
open Crdt.Protocol

[<RequireQualifiedAccess>]    
module MVRegister =
        
    type MVRegister<'a> = (VTime * 'a voption) list
    
    type Endpoint<'a> = Endpoint<MVRegister<'a>, 'a voption, 'a voption>
    
    let private crdt : Crdt<MVRegister<'a>, 'a list, 'a voption, 'a voption> =
        { new Crdt<_,_,_,_> with
            member _.Default = []
            member _.Query crdt =
                crdt
                |> List.choose (function (_, ValueSome v) -> Some v | _ -> None)
            member _.Prepare(_, value) = value
            member _.Effect(existing, e) =
                let concurrent =
                    existing
                    |> List.filter (fun (vt, _) -> Version.compare vt e.Version = Ord.Cc)
                (e.Version, e.Data)::concurrent }
    
    let props db replica ctx = replicator crdt db replica ctx
    let updte (value: 'a voption) (ref: Endpoint<'a>) : Async<'a voption> = ref <? Command value
    let query (ref: Endpoint<'a>) : Async<'a voption> = ref <? Query