/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "blockwise-rga.fsx"

module RGArrayTests =

  open Crdt
  open System

  let arr (s: string): char[] = s.ToCharArray()

  let insertApply replica value (at, rga) =
    let op = RGArray.insertAfter replica at value rga
    (op.At, RGArray.apply op rga)

  let removeApply at length rga = 
    let ops = RGArray.removeAt at length rga
    ops
    |> List.fold (fun rga op -> RGArray.apply op rga) rga

  let init () =
    (RGArray.head, RGArray.empty())
    |> insertApply "A" (arr "welcome")
    |> snd
    
  let ``RGA should allow to add elements to the tail`` () =
    let rga = init()
    String(RGArray.value rga) = "welcome"

  let ``RGA should allow to add elements to the head`` () =
    let rga =
      (RGArray.head,  init())
      |> insertApply "A" (arr "Very ")
      |> snd
    String(RGArray.value rga) = "Very welcome"
    
  let ``RGA should allow to add elements in the middle`` () =
    let rga = init()
    let id = RGArray.blockIdAtIndex 2 rga
    let rga' =
      (id.Value,  rga)
      |> insertApply "A" (arr "!abc!")
      |> snd
    String(RGArray.value rga') = "wel!abc!come"
    
  let ``RGA should allow to add elements in the middle multiple times`` () =
    let rga = init()
    let id = RGArray.blockIdAtIndex 2 rga
    let rga' =
      (id.Value,  rga)
      |> insertApply "A" (arr "!abc!")
      |> snd
    let id' = RGArray.blockIdAtIndex 9 rga'
    let rga'' =
      (id'.Value,  rga')
      |> insertApply "B" (arr "!def!")
      |> snd
    String(RGArray.value rga'') = "wel!abc!co!def!me"

open RGArrayTests

printfn "RGA should allow to add elements to the tail: %b" <| ``RGA should allow to add elements to the tail`` ()
printfn "RGA should allow to add elements to the head: %b" <| ``RGA should allow to add elements to the head`` ()
printfn "RGA should allow to add elements in the middle: %b" <| ``RGA should allow to add elements in the middle`` ()
printfn "RGA should allow to add elements in the middle multiple times: %b" <| ``RGA should allow to add elements in the middle multiple times`` ()