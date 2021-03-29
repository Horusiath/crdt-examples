/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.Pure.CounterTests


open System.Threading
open Expecto
open Akkling
open Crdt.Commutative.Pure

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative Counter" [
    test "should update and return values" {            
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Counter.props "A")
        
        let state = Counter.inc 1L a |> wait
        Expect.equal state 1L "counter state should be 1"
        
        let state = Counter.inc -2L a |> wait
        Expect.equal state -1L "counter state should be -1"
    }
    
    test "should propagate commutative updates" {        
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Counter.props "A")
        let b = spawn sys "B" <| props (Counter.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let s1 = Counter.inc 1L a |> wait        
        let s2 = Counter.inc -2L b |> wait
        Expect.equal s1 1L "A counter state after update should be 1"
        Expect.equal s2 -2L "B counter state after update should be -2"
        
        Thread.Sleep 1000
        
        let s1 = Counter.query a |> wait
        let s2 = Counter.query b |> wait
        
        Expect.equal s1 s2 "both replicas have the same value after replicating their updates"
    }
]
