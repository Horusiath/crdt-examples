/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../../common.fsx"

type Dot = ReplicaId * int64
type AWORSet<'a when 'a: comparison> = AWORSet of elements:Map<'a, Dot> * vector:VTime * delta:Delta<'a> option
and Delta<'a when 'a: comparison> =
    | Add of AWORSet<'a>
    | Remove of AWORSet<'a>
    | FullState of AWORSet<'a>
    | Group of Delta<'a> list

[<RequireQualifiedAccess>]
module AWORSet =

    let zero = AWORSet(Map.empty, Version.zero, None)
    
    let value (AWORSet(e, _, _)) =
        e 
        |> Map.toSeq
        |> Seq.map fst
        |> Set.ofSeq

    let add r (AWORSet(e, v, _)) =
        let c = 1L + Helpers.getOrElse r 0L v
        zero

    let remove r (AWORSet(e, v, _)) = zero

    let merge (AWORSet(e1, v1, _)) (AWORSet(e2, v2, _)) = zero

    let split (AWORSet(e, v, d)) = AWORSet(e, v, None), d