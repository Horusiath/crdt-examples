/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "aworset.fsx"

module AWORSetTests =
 
    open Crdt
    
    let ``AWORSet should add, remove and re-add elements in the same replica`` () = 
        let value =
            AWORSet.zero
            |> AWORSet.add "A" 1        // result: [1]
            |> AWORSet.add "A" 2        // result: [1,2]
            |> AWORSet.rem "A" 1        // result: [2]
            |> AWORSet.add "A" 3        // result: [2,3]
            |> AWORSet.add "A" 1        // result: [1,2,3]
            |> AWORSet.rem "A" 2        // result: [1,3]
            |> AWORSet.value
        value = Set.ofList [1;3]

    let ``AWORSet should prefer adds over rems in concurrent updates`` () = 
        let init =
            AWORSet.zero
            |> AWORSet.add "A" 1
            |> AWORSet.add "B" 2
        let a = 
            init
            |> AWORSet.rem "A" 1
            |> AWORSet.rem "A" 2
        let b =
            init 
            |> AWORSet.add "B" 1    // concurrent update with A:rem(1) - preferred add
            |> AWORSet.add "B" 3
        let value = 
            AWORSet.merge a b
        AWORSet.value value = Set.ofList [1;3]

    let ``AWORSet should merge in both directions`` () =
        let init =
            AWORSet.zero
            |> AWORSet.add "A" 1
            |> AWORSet.add "B" 2
        let a = 
            init
            |> AWORSet.rem "A" 1
            |> AWORSet.rem "A" 2
        let b =
            init 
            |> AWORSet.add "B" 1    // concurrent update with A:rem(1) - preferred add
            |> AWORSet.add "B" 3
        let ab = AWORSet.merge a b
        let ba = AWORSet.merge b a
        ab = ba &&
        AWORSet.value ab = AWORSet.value ba

open AWORSetTests

printfn "[TEST] AWORSet should add, remove and re-add elements in the same replica: %b" <| ``AWORSet should add, remove and re-add elements in the same replica``()
printfn "[TEST] AWORSet should prefer adds over rems in concurrent updates: %b" <| ``AWORSet should prefer adds over rems in concurrent updates``()
printfn "[TEST] AWORSet should merge in both directions: %b" <| ``AWORSet should merge in both directions``()