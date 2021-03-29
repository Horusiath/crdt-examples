/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.GSetTests

open Expecto
open Crdt.Convergent

[<Tests>]
let tests = testList "A convergent Grow-only Set" [            
    test "should add elements" {
        let value =
            GSet.zero
            |> GSet.add "a"
            |> GSet.add "c"
            |> GSet.add "b"
            |> GSet.add "c"
            |> GSet.value
            
        Expect.equal value (Set.ofList [ "a"; "b"; "c" ]) "GSet should return non-duplicate elements"
    }
    
    test "should merge both ways" {
        let init = GSet.add "x" GSet.zero
        let a = 
            init
            |> GSet.add "y"
            |> GSet.add "z"
        let b = 
            init
            |> GSet.add "u"
            |> GSet.add "y"
        let ab = GSet.merge a b
        let ba = GSet.merge b a
        
        Expect.equal ab ba "Both sets should be equal to each other after merge"
        Expect.equal (GSet.value ab) (GSet.value ba) "Both sets values should be equal to each other after merge"
    }
]