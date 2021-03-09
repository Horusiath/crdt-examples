module Crdt.Tests.Commutative.RGATests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative

[<Tests>]
let tests = testSequencedGroup "commutative" <| testList "A commutative Replicated Growable Array" [
    test "should allow to add multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Rga.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (Rga.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let s1 = Rga.insert 0 "A1" a |> wait
        let s2 = Rga.insert 0 "B1" b |> wait
        Expect.equal s1 [|"A1"|] "A should insert element at the 0 index"        
        Expect.equal s2 [|"B1"|] "B should insert element at the 0 index"
        
        Thread.Sleep 500
        
        let s1 = Rga.query a |> wait
        let s2 = Rga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after sync"
    }
    
    test "should allow to add and remove multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Rga.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (Rga.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        Rga.insert 0 "A1" a |> wait |> ignore
        Rga.insert 0 "B1" b |> wait |> ignore
        
        Thread.Sleep 500
        
        let s1 = Rga.insert 1 "A2" a |> wait
        let s2 = Rga.removeAt 1 b    |> wait
        Expect.equal s1 [|"B1";"A2";"A1"|] "A should insert second item into synced position"
        Expect.equal s2 [|"B1";|] "B should removed second item from synced position"
        
        Thread.Sleep 500
        
        let s1 = Rga.query a |> wait
        let s2 = Rga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after second sync"
    }
]