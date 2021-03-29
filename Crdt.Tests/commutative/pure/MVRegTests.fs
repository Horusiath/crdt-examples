/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.Pure.MVRegTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative.Pure

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative Multi-Value Register" [
    test "should propagate commutative updates" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (MVRegister.props "A")
        let b = spawn sys "B" <| props (MVRegister.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let s1 = MVRegister.update 1 a |> wait
        Expect.equal s1 [1] "A update returns latest value"

        let s2 = MVRegister.update 2 b |> wait     
        Expect.equal s2 [2] "B update returns latest value"
        
        Thread.Sleep 500
        
        let s1 = MVRegister.query a |> wait
        let s2 = MVRegister.query b |> wait
        
        Expect.equal s1 s2 "after replication both sides should have the same state"
    }
    
    test "should enable overriding conflicting updates" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (MVRegister.props "A")
        let b = spawn sys "B" <| props (MVRegister.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        MVRegister.update 1 a |> Async.RunSynchronously |> ignore        
        MVRegister.update 2 b |> Async.RunSynchronously |> ignore
        
        Thread.Sleep 500
        
        let state = MVRegister.query a |> Async.RunSynchronously
        Expect.equal state [1;2] "both conflicting values should be returned"
        
        let state = MVRegister.update 3 a |> Async.RunSynchronously
        Expect.equal state [3] "new update should override conflicting values"
        
        Thread.Sleep 500
        
        let state = MVRegister.query b |> Async.RunSynchronously
        Expect.equal state [3] "overriding update should be replicated and respected on replica B"
    }
]