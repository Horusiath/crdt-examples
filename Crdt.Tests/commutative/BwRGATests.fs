/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.BwRGATests

open System
open System.Threading
open Expecto
open Akkling
open Crdt.Commutative

let private arr (s: string) = s.ToCharArray()
let private str (s: char[]) = String(s)

[<Tests>]
let tests = testSequencedGroup "commutative" <| testList "A commutative Block-Wise Replicated Growable Array" [
    test "should allow to add multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (BWRga.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (BWRga.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let s1 = BWRga.insertRange 0 (arr "A1") a |> wait
        let s2 = BWRga.insertRange 0 (arr "B1") b |> wait
        Expect.equal (str s1) "A1" "A should insert element at the 0 index"        
        Expect.equal (str s2) "B1" "B should insert element at the 0 index"
        
        Thread.Sleep 500
        
        let s1 = BWRga.query a |> wait
        let s2 = BWRga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after sync"
    }
    
    test "should allow to add and remove multiple elements on the same position" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (BWRga.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (BWRga.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        BWRga.insertRange 0 (arr "A1") a |> wait |> ignore
        BWRga.insertRange 0 (arr "B1") b |> wait |> ignore
        
        Thread.Sleep 500
        
        let s1 = BWRga.insertRange 2 (arr "A2") a |> wait
        let s2 = BWRga.removeRange 1 2 b    |> wait
        Expect.equal (str s1) "B1A2A1" "A should insert second item into synced position"
        Expect.equal (str s2) "B1" "B should removed second item from synced position"
        
        Thread.Sleep 500
        
        let s1 = BWRga.query a |> wait
        let s2 = BWRga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after second sync"
    }
    
    test "should allow to concurrently remove one block and add another inside of it" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (BWRga.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (BWRga.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        BWRga.insertRange 0 (arr "ABC") a |> wait |> ignore
        BWRga.insertRange 3 (arr "DE") a |> wait |> ignore
        BWRga.insertRange 5 (arr "FGH") a |> wait |> ignore
        
        Thread.Sleep 500
        
        let s1 = BWRga.query a |> wait
        let s2 = BWRga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after sync"
        
        let s1 = BWRga.removeRange 2 4 a |> wait
        let s2 = BWRga.insertRange 4 (arr "__") b |> wait
        Expect.equal (str s1) "ABGH" "A should remove cross-blocks range"
        Expect.equal (str s2) "ABCD__EFGH" "B should insert __ within middle block"
        
        Thread.Sleep 500
        
        let s1 = BWRga.query a |> wait
        let s2 = BWRga.query b |> wait
        Expect.equal s1 s2 "both systems should have their states equal after second sync"
        Expect.equal (str s1) ("AB__GH") "__ inserted by B should be preserved"
    }
]