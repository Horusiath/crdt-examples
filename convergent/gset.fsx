/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type GSet<'a when 'a: comparison> = GSet of Set<'a>

[<RequireQualifiedAccess>]
module GSet =
    let zero: GSet<'a> = GSet Set.empty
    let value (GSet(s)) = s
    let add v (GSet(s)) = GSet (Set.add v s)
    let merge (GSet(a)) (GSet(b)) = GSet (a + b)