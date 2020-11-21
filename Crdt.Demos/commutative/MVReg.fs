/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Demos.CommutativeMVReg

open Akkling
open System
open Crdt.Commutative

let main () = async {
    use sys = System.create "sys" <| Configuration.parse "akka.loglevel = DEBUG"
    let a = spawn sys "A" <| props (MVRegister.props (InMemoryDb "A") "A")
    let b = spawn sys "B" <| props (MVRegister.props (InMemoryDb "B") "B")
    a <! Connect("B", b)
    b <! Connect("A", a)
                
    let! state = MVRegister.update 1L a
    printfn "State on node A (first): %A" state
    let! state = MVRegister.update 2L b
    printfn "State on node B (second): %A" state
    
    do! Async.Sleep 5000
    
    let! state = MVRegister.query b
    printfn "State on node B (after sync): %A" state
    let! state = MVRegister.update 3L b
    printfn "State on node B (after update): %A" state
    
    do! Async.Sleep 5000
    
    let! state1 = MVRegister.query a
    let! state2 = MVRegister.query b
    
    assert (state1 = state2)
    printfn "SUCCESS"
}