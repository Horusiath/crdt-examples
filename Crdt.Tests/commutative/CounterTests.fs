module Crdt.Tests.Commutative.CounterTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative

[<Tests>]
let tests = testSequencedGroup "commutative" <| testList "A commutative Counter" [
    test "should update and return values" {            
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Counter.props (InMemoryDb "A") "A")
        
        let state = Counter.inc 1L a |> wait
        Expect.equal state 1L "counter state should be 1"
        
        let state = Counter.inc -2L a |> wait
        Expect.equal state -1L "counter state should be -1"
    }
    
    test "should propagate commutative updates" {        
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Counter.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (Counter.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let state = Counter.inc 1L a |> wait
        Expect.equal state 1L "A counter state after update should be 1"
        
        let state = Counter.inc -2L b |> wait
        Expect.equal state -2L "B counter state after update should be -2"
        
        Thread.Sleep 500
        
        let s1 = Counter.query a |> wait
        let s2 = Counter.query b |> wait
        
        Expect.equal s1 s2 "both replicas have the same value after replicating their updates"
    }
]
