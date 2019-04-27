/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type MVReg<'a when 'a: comparison> = MVReg of Set<'a * VTime>

[<RequireQualifiedAccess>]
module MVReg =

  type Op<'a> = Assign of 'a

  let empty = MVReg Set.empty

  let value (MVReg v) = 
    if Set.isEmpty v then None
    else v |> Set.map fst |> Set.minElement

  let assign e = ???

  let downstream (MVReg v) op = 
    match op with
    | Assign e -> 