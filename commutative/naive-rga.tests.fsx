/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "naive-rga.fsx"

module RGArrayTests =

  open Crdt
  open System

  let insertApply replica value (pos, rga) =
    let op = RGArray.insertAfter replica pos value rga
    (op.At, RGArray.apply op rga)

  let removeApply pos rga = 
    let op = RGArray.removeAt pos rga
    RGArray.apply op rga

  let str rga = new String(RGArray.value rga)

  let init () =
    (RGArray.head, RGArray.empty())
      |> insertApply "A" 'h' 
      |> insertApply "A" 'e'
      |> insertApply "A" 'l'
      |> insertApply "A" 'l'
      |> insertApply "A" 'o'
      |> snd
      
  let ``RGArray should allow to add elements to the tail`` () =
    let rga = init ()      
    let value = str rga
    value = "hello"

  let ``RGArray should allow to add elements to the head`` () =
    let rga = init ()     
    let rga' = 
      (RGArray.head, rga)
      |> insertApply "A" '!'
      |> snd
    let value = str rga'
    value = "!hello"
    
  let ``RGArray should allow to add elements in the middle`` () =
    let rga = init ()
    let pos = RGArray.positionAtIndex 2 rga |> Option.get
    let rga' =
      (pos, rga)
      |> insertApply "A" 'b'
      |> insertApply "A" 'r'
      |> insertApply "A" 'e'
      |> insertApply "A" 'a'
      |> insertApply "A" 'k'
      |> snd
    let value = str rga'
    value = "helbreaklo"

  let ``RGArray should allow to remove elements from the head`` () =
    let rga = init ()      
    let rga' = rga |> removeApply (RGArray.positionAtIndex 0 rga).Value
    let rga'' =  rga' |> removeApply (RGArray.positionAtIndex 0 rga').Value
    let value = str rga''
    value = "llo"
    
  let ``RGArray should allow to remove elements from the tail`` () =
    let rga = init ()     
    let rga' = rga |> removeApply (RGArray.positionAtIndex 4 rga).Value
    let rga'' =  rga' |> removeApply (RGArray.positionAtIndex 3 rga').Value
    let value = str rga''
    value = "hel"
    
  let ``RGArray should allow to remove elements from the middle`` () =
    let rga = init ()      
    let rga' = rga |> removeApply (RGArray.positionAtIndex 2 rga).Value
    let rga'' =  rga' |> removeApply (RGArray.positionAtIndex 2 rga').Value
    let value = str rga''
    value = "heo"
    
  let ``RGArray should allow concurrent inserts`` () =
    let rga = init ()  
    let (Some pos) = RGArray.positionAtIndex 2 rga   
    let a = (pos, rga) |> insertApply "B" 'x' |> snd
    let b = (pos, a) |> insertApply "A" 'y' |> snd
    let value = str b
    value = "helyxlo"
    
  let ``RGArray should allow concurrent inserts and removals`` () =
    let rga = init ()  
    let (Some pos) = RGArray.positionAtIndex 2 rga   
    let a = rga |> removeApply pos
    let b = (pos, a) |> insertApply "A" 'y' |> snd
    let value = str b
    value = "heylo"

open RGArrayTests

printfn "[TEST] RGArray should allow to add elements to the tail: %b" <| ``RGArray should allow to add elements to the tail`` ()
printfn "[TEST] RGArray should allow to add elements to the head: %b" <| ``RGArray should allow to add elements to the head`` ()
printfn "[TEST] RGArray should allow to add elements in the middle: %b" <| ``RGArray should allow to add elements in the middle`` ()
printfn "[TEST] RGArray should allow to remove elements from the head: %b" <| ``RGArray should allow to remove elements from the head`` ()
printfn "[TEST] RGArray should allow to remove elements from the tail: %b" <| ``RGArray should allow to remove elements from the tail`` ()
printfn "[TEST] RGArray should allow to remove elements from the middle: %b" <| ``RGArray should allow to remove elements from the middle`` ()
printfn "[TEST] RGArray should allow concurrent inserts: %b" <| ``RGArray should allow concurrent inserts`` ()
printfn "[TEST] RGArray should allow concurrent inserts and removals: %b" <| ``RGArray should allow concurrent inserts and removals`` ()