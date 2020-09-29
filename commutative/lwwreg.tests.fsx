/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

#load "lwwreg.fsx"

module LWWRegisterTests = 

    open Akkling
    open Crdt
    
    let main () = async {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = DEBUG"
        let a = spawn sys "A" <| props (LWWRegister.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (LWWRegister.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
                    
        let! state = LWWRegister.update 1L a
        printfn "State on node A (first): %A" state
        let! state = LWWRegister.update 2L b
        printfn "State on node B (second): %A" state
        
        do! Async.Sleep 5000
        
        let! state = LWWRegister.query b
        printfn "State on node B (after sync): %A" state
        let! state = LWWRegister.update 3L b
        printfn "State on node B (after update): %A" state
        
        do! Async.Sleep 5000
        
        let! state1 = LWWRegister.query a
        let! state2 = LWWRegister.query b
        
        assert (state1 = state2)
        printfn "SUCCESS"
    }

open LWWRegisterTests

main () |> Async.RunSynchronously