/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.Pure.EvictionTests

open System.Collections.Concurrent
open System.Threading
open Akka.Actor
open Expecto
open Akkling
open Crdt.Commutative.Pure

type InboxMsg<'op> =
    | Terminated of IActorRef
    | Event of Event<'op>

let private inbox (fac: #IActorRefFactory) =
    let q = ConcurrentQueue<_>()
    let rec loop (q: ConcurrentQueue<InboxMsg<'op>>) (ctx: Actor<_>) (m: obj) =
        match m with
        | :? Event<'op> as e -> q.Enqueue (Event e)
        | :? Terminated as t -> q.Enqueue (Terminated t.ActorRef)
        | :? IActorRef as a -> ctx.Watch(a) |> ignore
        | _ -> ()
        become (loop q ctx)
        
    let ref = spawnAnonymous fac <| props (actorOf2 (loop q))
    (q, ref)

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative node eviction" [
    test "evicted node should publish concurrent events and stop" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        
        let (q, i) = inbox sys
        subscribe (retype i) sys.EventStream |> ignore
        
        let a = spawn sys "A" <| props (Counter.props "A")
        let b = spawn sys "B" <| props (Counter.props "B")
        
        i <! b
                
        Counter.inc 1L a |> wait |> ignore
        Counter.inc 2L b |> wait |> ignore
        
        a <! Connect("B", b)
        a <! Evict "B"
        
        Thread.Sleep 500
        
        for msg in q do
            match msg with
            | Terminated x -> Expect.equal x (untyped b) "evicted actor should be stopped"
            | Event e -> Expect.equal e.Value 2L "concurrent event should be evacuated"
    }
    
    test "evicted node should be able to reappear after reset" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        
        let a = spawn sys "A" <| props (ORSet.props "A")
        let b = spawn sys "B" <| props (ORSet.props "B")
        let c = spawn sys "C" <| props (ORSet.props "C")
        
        let nodes = [|
            "A", a
            "B", b
            "C", c
        |]
        
        for (id1, ref1) in nodes do
            for (id2, ref2) in nodes do
                if id1 <> id2 then
                    ref1 <! Connect(id2, ref2)
                    ref2 <! Connect(id1, ref1)
        
        for (id, ref) in nodes do
            ORSet.add id ref |> wait |> ignore
            
        a <! Evict "C"
        
        Thread.Sleep 500
        
        let state = ORSet.query b |> wait
        Expect.equal state (Set.ofList ["A";"B"]) "C should not be replicated"
        
        let c = spawn sys "C" <| props (ORSet.props "C")
        a <! Connect("C", c)
        b <! Connect("C", c)
        c <! Connect("A", a)
        c <! Connect("B", b)
        
        // we cannot progress before 
        Thread.Sleep 500
        
        let state = ORSet.add "C" c |> wait
        Expect.equal state (Set.ofList ["A";"B";"C"]) "C should be added after eviction"
        
        Thread.Sleep 500
        
        let state = ORSet.query a |> wait
        Expect.equal state (Set.ofList ["A";"B";"C"]) "C should be replicated after eviction and reset"
        
    }
]