/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

module Crdt.Demos.CommutativeBwRga

open Akkling
open System
open Crdt.Commutative

let inline str (s: string) = s.ToCharArray()

let main () = async {
    use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
    let a = spawn sys "A" <| props (BWRga.props (InMemoryDb "A") "A")
    let b = spawn sys "B" <| props (BWRga.props (InMemoryDb "B") "B")
    a <! Connect("B", b)
    b <! Connect("A", a)
                
    let! state = a |> BWRga.insertRange 0 (str "Hello!")
                
    do! Async.Sleep 5000
    
    let! state1 = a |> BWRga.insertRange 5 (str " Alice")
    let! state2 = b |> BWRga.insertRange 5 (str " Bob")
    
    printfn "A state (after insert): '%s'" (String state1)
    printfn "B state (after insert): '%s'" (String state2) 
    
    do! Async.Sleep 5000
    
    let! state1 = a |> BWRga.removeRange 6 7
    let! state2 = b |> BWRga.insertRange 5 (str " Big")
    
    printfn "A state (after remove): '%s'" (String state1)
    printfn "B state (after insert): '%s'" (String state2) 
    
    do! Async.Sleep 5000
    
    let! state1 = BWRga.query a
    let! state2 = BWRga.query b
    
    printfn "A state (final): '%s'" (String state1)
    printfn "B state (final): '%s'" (String state2) 
    
    assert (state1 = state2)
    printfn "SUCCESS"
}