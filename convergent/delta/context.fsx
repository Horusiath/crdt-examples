/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "../../common.fsx"

namespace Crdt

type Dot = ReplicaId * int64
type DotContext = 
    { /// A complete history representing a set of consecutive 
      /// sequence numbers up to a given point in time.
      Clock: Map<ReplicaId, int64>

      /// A pairs of dots that represent non-consecutive event,
      /// therefore they cannot be compacted to `Clock`.
      DotCloud: Set<Dot> }

[<RequireQualifiedAccess>]
module DotContext =

    let zero = { Clock = Map.empty; DotCloud = Set.empty }

    /// Checks if given dot is present inside of a provided context.
    let contains (r, n) ctx = 
        match Map.tryFind r ctx.Clock with
        | Some n2 when n2 >= n -> true
        | _ -> Set.contains (r, n) ctx.DotCloud

    /// Checks if it's possible to line up the dots living inside
    /// DotCloud into a monotonic sequence by replica id. If so, 
    /// they will be absorbed into Clock, reducing memory footprint.
    let compact ctx =
        let (clock, removeDots) = 
            ctx.DotCloud
            |> Set.fold (fun (cc, rem) (r,n) -> 
                // try to find dot from DotCloud in Clock (use 0 if not found)
                let n2 = defaultArg (Map.tryFind r cc) 0L
                // 1. if current dot is exactly next after corresponding
                // value in the clock, remove it from DotCloud and add to
                // clock itself
                if n = n2 + 1L then (Map.add r n cc, Set.add (r,n) rem)
                // 2. if current dot is behind clock, it's outdated
                // in this case remove it from dotCloud
                elif n <= n2   then (cc, Set.add (r,n) rem)
                // 3. if it was not next or behind, it cannot be squashed
                // in that case leave state unchanged
                else (cc, rem)
            ) (ctx.Clock, Set.empty)
        { Clock = clock; DotCloud = ctx.DotCloud - removeDots }

    /// Picks a next dot for a given replica `r` inside of a used
    /// context. Returns dot and updated context in result.
    let nextDot r (ctx): Dot * DotContext = 
        let newCtx = { ctx with Clock = Helpers.upsert r 1L ((+)1L) ctx.Clock }
        ((r, newCtx.Clock.[r]), newCtx)

    let add dot ctx = { ctx with DotCloud = Set.add dot ctx.DotCloud }

    let merge a b =
        let mergeMap x y = 
            y |> Map.fold (fun acc k v -> Helpers.upsert k v (max v) acc) x
        let clock = mergeMap a.Clock b.Clock
        let cloud = a.DotCloud + b.DotCloud
        { Clock = clock; DotCloud = cloud } |> compact