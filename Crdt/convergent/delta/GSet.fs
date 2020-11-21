/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Convergent.Delta

open Crdt

type GSet<'a when 'a: comparison> = GSet of values:Set<'a> * delta:GSet<'a> option

/// A delta-state based implementation of immutable grow-only set.
[<RequireQualifiedAccess>]
module GSet =

    /// Returns a default, empty instance of G-Set.
    let zero = GSet(Set.empty, None)

    /// Returns a value of provided G-Set.
    let value (GSet(s, _)) = s

    /// Adds an element to provided G-Set, returning a new G-Set instance.
    let add elem (GSet(v, d)) =
        let (GSet(delta, None)) = defaultArg d zero
        GSet(Set.add elem v, Some(GSet(Set.add elem delta, None)))

    /// Merges two G-Sets together. Result is a new G-Set instance with union of elements from both inputs.
    let rec merge (GSet(a, da)) (GSet(b, db)) = 
        let values = a + b
        // starting from here to the end of the snipped, 
        // the code is the same as in case of GCounter
        let delta = Helpers.mergeOption merge da db
        GSet(values, delta)

    /// Merges given G-Set delta with G-Set instance.
    let mergeDelta delta gset = merge gset delta

    /// Splits a given G-Set into G-Set with prunned delta and its delta, if provided.
    let split (GSet(v, d)) = GSet(v, None), d
