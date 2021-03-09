module Crdt.Tests.Commutative.LWWRegTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative

[<Tests>]
let tests = testSequencedGroup "commutative" <| testList "A commutative Last-Write-Wins Register" [
    test "should return the latest value" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (LWWRegister.props (InMemoryDb "A") "A")
        
        let state = LWWRegister.query a |> wait
        Expect.equal state ValueNone "new register has no value"
        
        let state = LWWRegister.update 1 a |> wait
        Expect.equal state (ValueSome 1) "register value after 1st update is 1"
        
        let state = LWWRegister.update 2 a |> wait
        Expect.equal state (ValueSome 2) "register value after 2nd update is 2"
        
        let state = LWWRegister.query a |> wait
        Expect.equal state (ValueSome 2) "register value is the value of 2nd update"
    }

    test "should propagate commutative updates" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (LWWRegister.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (LWWRegister.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let state = LWWRegister.update 1 a |> wait
        Expect.equal state (ValueSome 1) "update returns latest value"
        
        let state = LWWRegister.update 2 b |> wait
        Expect.equal state (ValueSome 2) "update returns latest value"
        
        Thread.Sleep 500
        
        let s1 = LWWRegister.query a |> wait
        let s2 = LWWRegister.query b |> wait
        
        Expect.equal s1 s2 "after replication both sides should have the same state"
    }
]
