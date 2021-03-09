/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative

open Crdt
open Akkling

[<RequireQualifiedAccess>]    
module MVRegister =
        
    type MVRegister<'a> = (VTime * 'a) list
    
    type Endpoint<'a> = Endpoint<MVRegister<'a>, 'a, 'a>
    
    let private crdt : Crdt<MVRegister<'a>, 'a list, 'a, 'a> =
        { new Crdt<_,_,_,_> with
            member _.Default = []
            member _.Query crdt = crdt |> List.map snd |> List.sort
            member _.Prepare(_, value) = value
            member _.Effect(existing, e) =
                let concurrent =
                    existing
                    |> List.filter (fun (vt, _) -> Version.compare vt e.Version = Ord.Cc)
                (e.Version, e.Data)::concurrent }
    
    let props db replica ctx = replicator crdt db replica ctx
    let update (value: 'a) (ref: Endpoint<'a>) : Async<'a list> = ref <? Command value
    let query (ref: Endpoint<'a>) : Async<'a list> = ref <? Query