/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.CommutativeORSet

open Akkling
open System
open Crdt.Commutative

let main () = async {
    use sys = System.create "sys" <| Configuration.parse "akka.loglevel = DEBUG"
    let a = spawn sys "A" <| props (ORSet.props (InMemoryDb "A") "A")
    let b = spawn sys "B" <| props (ORSet.props (InMemoryDb "B") "B")
    a <! Connect("B", b)
    b <! Connect("A", a)
                
    let! state = ORSet.add 1L a
    printfn "State on node A (first): %A" state
    
    do! Async.Sleep 1000
    
    let! state = ORSet.add 2L a
    printfn "State on node A (second): %A" state
    
    do! Async.Sleep 5000
    
    let! state = ORSet.query b
    printfn "State on node B (after sync): %A" state
    let! state = ORSet.remove 2L b
    printfn "State on node B (after update): %A" state
    
    do! Async.Sleep 5000
    
    let! state1 = ORSet.query a
    let! state2 = ORSet.query b
    
    assert (state1 = state2)
    printfn "SUCCESS"
}