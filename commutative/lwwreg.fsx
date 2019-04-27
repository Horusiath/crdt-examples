/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

open System

#load "../common.fsx"

type LWWReg<'a when 'a: comparison> = LWWReg of Set<DateTime * 'a>

[<RequireQualifiedAccess>]
module LWWReg =

  type Op<'a> = Assign of DateTime * 'a

  let empty: LWWReg<'a> = LWWReg Set.empty

  let value (LWWReg s) =
    if Set.isEmpty s then None
    else s |> Set.maxElement |> snd

  let set ts e = Assign(ts,e)

  let downstream (LWWReg s) (Assign (ts, e)) = LWWReg (Set.add (ts,e) s)
