/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.DocTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative

let inline private (~%%) x = Unchecked.defaultof<Primitive> $ x
let inline private field name inner = Doc.Update(name, inner)
let inline private set idx inner = Doc.UpdateAt(idx, inner)
let inline private insert idx inner = Doc.InsertAt(idx, inner)
let inline private assign value = Doc.Assign(%% value)

[<Tests>]
let tests = testSequencedGroup "commutative" <| testList "A commutative JSON document CRDT" [
    test "should allow to concurrently add inner node and remove outer one" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (Doc.props (InMemoryDb "A") "A")
        let b = spawn sys "B" <| props (Doc.props (InMemoryDb "B") "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
             
        let s1 = Doc.request (field "friends" << insert 0 << field "name" <| assign "Alice") a |> wait
        let s2 = Doc.request (field "friends" << insert 0 << field "name" <| assign "Bob") b |> wait
        
        Expect.equal (string s1) "{friends: [{name: 'Alice'}]}" "A state after update"
        Expect.equal (string s2) "{friends: [{name: 'Bob'}]}" "B state after update"
        
        Thread.Sleep 500
        let s1 = Doc.query a |> wait  
        let s2 = Doc.query b |> wait  
        
        Expect.equal s1 s2 "both replicas should have equal state after sync"
        Expect.equal (string s1) "{friends: [{name: 'Bob'}, {name: 'Alice'}]}" "state after sync on both sides"
        
        let s1 = Doc.request (field "friends" <| set 0 Doc.Remove) a |> wait
        let s2 = Doc.request (field "friends" << set 0 << field "surname" <| assign "Dylan") b |> wait
        
        Expect.equal (string s1) "{friends: [{name: 'Alice'}]}" "A state after remove"
        Expect.equal (string s2) "{friends: [{name: 'Bob', surname: 'Dylan'}, {name: 'Alice'}]}" "B state after adding surname"
        
        Thread.Sleep 500
        // after sync on both sides: {friends: [{surname: Dylan}, {name: Alice]}
        
        let s1 = Doc.query a |> wait
        let s2 = Doc.query b |> wait
        
        Expect.equal s1 s2 "eventually both nodes should produce the same state"
        Expect.equal (string s1) "{friends: [{surname: 'Dylan'}, {name: 'Alice'}]}" "final state after last sync"
    }
]