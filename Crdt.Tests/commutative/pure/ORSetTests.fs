module Crdt.Tests.Commutative.Pure.ORSetTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative.Pure

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative Add-Wins Observed Remove Set" [
    
    test "should allow to add and remove values" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (ORSet.props "A")
        
        let state = ORSet.add 1 a |> wait
        Expect.equal state (Set.singleton 1) "A returns added value"
        
        let state =  ORSet.remove 1 a |> wait
        Expect.equal state Set.empty "A returns set without removed value"
        
        let state = ORSet.add 1 a |> wait
        Expect.equal state (Set.singleton 1) "A returns set with old value added again"
    }
    
    test "should prefer adds over removals" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (ORSet.props "A")
        let b = spawn sys "B" <| props (ORSet.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let state = ORSet.add 1 a |> wait
        Expect.equal state (Set.singleton 1) "A returns added value"
        
        let state = ORSet.add 2 b |> wait
        Expect.equal state (Set.singleton 2) "B returns added value"
        
        ORSet.add 1 b |> wait |> ignore
        ORSet.remove 1 a |> wait |> ignore
        
        Thread.Sleep 500
        
        let s1 = ORSet.query a |> wait
        let s2 = ORSet.query b |> wait
        
        Expect.equal s1 s2 "after replication both sides should have the same state"
        Expect.equal s1 (Set.ofList [1;2]) "on concurrent conflicting update, add wins"
    }
]