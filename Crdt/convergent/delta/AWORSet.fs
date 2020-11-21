/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Convergent.Delta

open Crdt

type AWORSet<'a when 'a: comparison> = AWORSet of core:DotKernel<'a> * delta:DotKernel<'a> option

[<RequireQualifiedAccess>]
module AWORSet =

    let zero = AWORSet(DotKernel.zero, None)
    
    let value (AWORSet(k, _)) =
        k 
        |> DotKernel.values
        |> Set.ofSeq

    let add r v (AWORSet(k, d)) =
        let (k2, d2) = DotKernel.remove r v (k, defaultArg d DotKernel.zero)
        let (k3, d3) = DotKernel.add r v (k2, d2)
        AWORSet(k3, Some d3)

    let rem r v (AWORSet(k, d)) =
        let (k2, d2) = DotKernel.remove r v (k, defaultArg d DotKernel.zero)
        AWORSet(k2, Some d2)

    let merge (AWORSet(ka, da)) (AWORSet(kb, db)) = 
        let dc = Helpers.mergeOption DotKernel.merge da db
        let kc = DotKernel.merge ka kb
        AWORSet(kc, dc)

    let mergeDelta (AWORSet(ka, da)) (delta) =
        let dc = Helpers.mergeOption DotKernel.merge da (Some delta)
        let kc = DotKernel.merge ka delta
        AWORSet(kc, dc)

    let split (AWORSet(k, d)) = AWORSet(k, None), d