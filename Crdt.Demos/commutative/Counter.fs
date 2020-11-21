/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Demos.CommutativeCounter


open Akkling
open Crdt.Commutative

let main () = async {
    use sys = System.create "sys" <| Configuration.parse "akka.loglevel = DEBUG"
    let a = spawn sys "A" <| props (Counter.props (InMemoryDb "A") "A")
    let b = spawn sys "B" <| props (Counter.props (InMemoryDb "B") "B")
    a <! Connect("B", b)
    b <! Connect("A", a)
    
    let! state = Counter.inc 1L a
    printfn "State on node A (first): %A" state
    
    do! Async.Sleep 1000
    
    let! state = Counter.inc -2L a
    printfn "State on node A (second): %A" state
    
    do! Async.Sleep 5000
    
    let! state = Counter.query b
    printfn "State on node B (after sync): %A" state
    let! state = Counter.inc 2L b
    printfn "State on node B (after update): %A" state
    
    do! Async.Sleep 5000
    
    let! state1 = Counter.query a
    let! state2 = Counter.query b
    
    assert (state1 = state2)
    printfn "SUCCESS"
}