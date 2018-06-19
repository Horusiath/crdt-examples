/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "kernel.fsx"

type MVReg<'a> when 'a: equality = MVReg of core:DotKernel<'a> * delta:DotKernel<'a> option

[<RequireQualifiedAccess>]
module MVReg =
    
    let zero: MVReg<_> = MVReg(DotKernel.zero, None)

    let value (MVReg(k, _)) = 
        DotKernel.values k 
        |> Seq.distinct 
        |> Seq.toList

    let set r v (MVReg(k, d)) =
        let (k2, d2) = DotKernel.removeAll (k, defaultArg d DotKernel.zero)
        let (k3, d3) = DotKernel.add r v (k2, d2)
        MVReg(k3, Some d3)

    let merge (MVReg(ka, da)) (MVReg(kb, db)) = 
        let dc = Helpers.mergeOption DotKernel.merge da db
        let kc = DotKernel.merge ka kb
        MVReg(kc, dc)

    let mergeDelta (MVReg(ka, da)) (delta) =
        let dc = Helpers.mergeOption DotKernel.merge da (Some delta)
        let kc = DotKernel.merge ka delta
        MVReg(kc, dc)

    let split (MVReg(k, d)) = MVReg(k, None), d
