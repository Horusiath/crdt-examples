/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type Pos = ReplicaId * int
type Block<'a> =
    | Data of offset:int * values:'a[] * next:Block<'a> option
    | Tombstone of offset:int * deletionTime:VTime * next:Block<'a> option

type RGArray<'a> = RGArray of Map<Pos, Block<'a>>

[<RequireQualifiedAccess>]
module RGArray = 
    let zero = RGArray Map.empty
