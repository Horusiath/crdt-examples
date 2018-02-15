/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

open System

type LWWReg<'a> = LWWReg of 'a * DateTime

[<RequireQualifiedAccess>]
module LWWReg =
    let zero(): LWWReg<_> = LWWReg(Unchecked.defaultof<'a>, DateTime.MinValue)
    let value (LWWReg(v, _)) = v
    let set c2 v2 (LWWReg(v1, c1)) = if c1 < c2 then LWWReg(v2, c2) else LWWReg(v1, c1)
    let merge (LWWReg(v1, c1)) (LWWReg(v2, c2)) = if c1 < c2 then (v2, c2) else (v1, c1)