/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "context.fsx"

type DotKernel<'a when 'a: equality> =
    { /// Set of all dots that have been ever seen. If a dot is not
      /// present in Elements map, it means it has been removed.
      Context: DotContext
      
      /// Active set of live elements and their dots describing their
      /// actual position in causal timeline.
      Entries: Map<Dot, 'a> }

[<RequireQualifiedAccess>]
module DotKernel =

    /// Returns a zero element of the kernel (an empty one).
    let zero: DotKernel<'a> = { Context = DotContext.zero; Entries = Map.empty }

    /// Returns active value set of the current kernel.
    let values kernel = 
        kernel.Entries 
        |> Map.toSeq 
        |> Seq.map snd

    /// Adds a value `v` under provided replica `rep` to the kernel `k` and its delta `d`.
    let add rep v (k, d) =
        let (dot, ctx) = DotContext.nextDot rep k.Context
        let kernel = { k with Entries = Map.add dot v k.Entries; Context = ctx }
        let delta = { d with 
            Entries = Map.add dot v d.Entries; 
            Context = DotContext.add dot d.Context 
                      |> DotContext.compact }
        (kernel, delta)

    /// Removes a value `v` in context of replica `rep` from kernel `k`. Changes
    /// are remembered within delta `d`.
    let remove rep v (k, d) = 
        let (entries, deltaCtx) =
            k.Entries 
            |> Map.fold (fun (e, dc) dot v2 ->
                if v2 = v 
                then (Map.remove dot e, DotContext.add dot dc)
                else (e, dc)
            ) (k.Entries, d.Context)
        ({ k with Entries = entries }, { d with Context = deltaCtx |> DotContext.compact })

    /// Removes all values from kernel, but maintains its context.
    let removeAll (k, d) =
        let deltaCtx = 
            k.Entries 
            |> Map.fold (fun acc dot _ -> DotContext.add dot acc) d.Context
        ({ k with Entries = Map.empty }, { d with Context = deltaCtx |> DotContext.compact })

    let merge a b =
        // first add only those elements, that haven't been seen in 
        // current history of dots
        let active =
            b.Entries
            |> Map.fold (fun acc dot v -> 
                if not <| (Map.containsKey dot a.Entries || DotContext.contains dot a.Context)
                then Map.add dot v acc else acc) a.Entries
        // then remove all active entries that are not not visible in
        // b's entries but are in b's context (it means they were removed)
        let final = 
            a.Entries
            |> Map.fold (fun acc dot _ ->
                if DotContext.contains dot b.Context && not <| Map.containsKey dot b.Entries
                then Map.remove dot acc else acc) active
        { Entries = final; Context = DotContext.merge a.Context b.Context }