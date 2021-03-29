/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Demos.CommutativeDoc

open Crdt.Commutative
open Crdt.Commutative.Doc
open Akkling

let inline (~%%) x = Unchecked.defaultof<Primitive> $ x
    
let main () = async {
    use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
    let a = spawn sys "A" <| props (Doc.props (InMemoryDb "A") "A")
    let b = spawn sys "B" <| props (Doc.props (InMemoryDb "B") "B")
    a <! Connect("B", b)
    b <! Connect("A", a)
         
    let! state1 =  request (Update ("friends", InsertAt(0, Update("name", Assign (%% "Alice"))))) a
    let! state2 =  request (Update ("friends", InsertAt(0, Update("name", Assign (%% "Bob"))))) b
    
    printfn "A after insert: %O" state1 // {friends: [{name: Alice}]}
    printfn "B after insert: %O" state2 // {friends: [{name: Bob}]}
    
    do! Async.Sleep 6_000
    // after sync on both sides: {friends: [{name: Bob}, {name: Alice}]}
    let! state1 = query a  
    let! state2 = query b  
    
    printfn "A stable (1): %O" state1
    printfn "B stable (1): %O" state2
    
    let! state1 = request (Update ("friends", UpdateAt(0, Remove))) a
    let! state2 = request (Update ("friends", UpdateAt(0, Update("surname", Assign (%% "Dylan"))))) b
    
    printfn "A after remove: %O" state1 // {friends: [{name: Alice}]}
    printfn "B after update: %O" state2 // {friends: [{name: Bob, surname: Dylan}, {name: Alice}]}
    
    do! Async.Sleep 6_000
    // after sync on both sides: {friends: [{surname: Dylan}, {name: Alice]}
    
    let! state1 = query a
    let! state2 = query b
    
    printfn "A stable (2): %O" state1
    printfn "B stable (2): %O" state2
    
    assert (state1 = state2)
    printfn "SUCCESS"
}