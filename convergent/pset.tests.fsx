/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "pset.fsx"

module PSetTests =

    let ``2-Phase set should be able to add and remove elements and prefer removal`` () =
        let value = 
            PSet.zero
            |> PSet.add "a"     // expected value: [a]
            |> PSet.add "b"     // expected value: [a,b]
            |> PSet.rem "c"     // expected value: [a,b] - [c] remembered in tombstones
            |> PSet.rem "a"     // expected value: [b]   - [a,c] remembered in tombstones
            |> PSet.add "c"     // expected value: [b]   - cannot add c, once it's tombstoned
            |> PSet.add "a"     // expected value: [b]   - cannot readd a, once it's tombstoned
            |> PSet.value
        value = Set.singleton "b"

    let ``2-Phase set should merge in both directions`` () =
        failwith "not implemented"