/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Convergent

open Crdt

open System

type LWWReg<'a> = LWWReg of 'a * DateTime

[<RequireQualifiedAccess>]
module LWWReg =
    let zero(): LWWReg<_> = LWWReg(Unchecked.defaultof<'a>, DateTime.MinValue)
    let value (LWWReg(v, _)) = v
    let set clock value (LWWReg(v1, c1)) = if c1 < clock then LWWReg(value, clock) else LWWReg(v1, c1)
    let merge (LWWReg(v1, c1)) (LWWReg(v2, c2)) = if c1 < c2 then LWWReg(v2, c2) else LWWReg(v1, c1)
    
    [<Struct>]
    type Merge<'a> = 
        interface IConvergent<LWWReg<'a>> with
            member __.merge a b = merge a b 