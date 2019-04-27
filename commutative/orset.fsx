/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type ORSet<'a when 'a: comparison> = ORSet of Set<'a * VTime>

[<RequireQualifiedAccess>]
module ORSet =

  /// Operation to be transmitted and applied by all CRDT `ORSet` replicas.
  type Op<'a when 'a: comparison> = 
    | Add of 'a * VTime
    | Remove of VTime

  /// Default instance of Observed-Remove Set.
  let empty: ORSet<'a> = ORSet Set.empty

  /// Returns a value of Observed-Remove Set.
  let value (ORSet s) = s |> Set.map fst

  let add ts e = Add(e, ts)
  
  let rem ts = Remove ts
  
  let downstream (ORSet s) op = 
    match op with
    | Add (e, ts) -> Set.add (e,ts) s
    | Remove ts -> s |> Set.filter (fun (_, ts') -> ts' <> ts)
    |> ORSet