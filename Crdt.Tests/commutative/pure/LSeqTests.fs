/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.commutative.Pure.LSeqTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative.Pure

let wait = Async.RunSynchronously

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative Linear Sequence" [
    test "should allow to add multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (LSeq.props "A")
        let b = spawn sys "B" <| props (LSeq.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let s1 = LSeq.insert 0 "A1" a |> wait // [A1]
        let s2 = LSeq.insert 0 "B1" b |> wait // [B1]
        Expect.equal s1 [|"A1"|] "A should insert element at the 0 index"        
        Expect.equal s2 [|"B1"|] "B should insert element at the 0 index"
        
        Thread.Sleep 500
        
        let s1 = LSeq.query a |> wait // [B1,A1]
        let s2 = LSeq.query b |> wait // [B1,A1]
        Expect.equal s1 s2 "both systems should have their states equal after sync"
    }
    
    ftest "should allow to add and remove multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = DEBUG"
        let a = spawn sys "A" <| props (LSeq.props "A")
        let b = spawn sys "B" <| props (LSeq.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        LSeq.insert 0 "A1" a |> wait |> ignore // [A1]
        LSeq.insert 0 "B1" b |> wait |> ignore // [B1]
        
        Thread.Sleep 500
        // [B1,A1]
        
        let s1 = LSeq.insert 1 "A2" a |> wait  // [B1,A2,A1]
        let s2 = LSeq.removeAt 1 b    |> wait  // [B1]
        Expect.equal s1 [|"B1";"A2";"A1"|] "A should insert second item into synced position"
        Expect.equal s2 [|"B1";|] "B should removed second item from synced position"
        
        Thread.Sleep 500
        
        let s1 = LSeq.query a |> wait // [B1,A2]
        let s2 = LSeq.query b |> wait // [B1,A2]
        Expect.equal s1 s2 "both systems should have their states equal after second sync"
    }
]