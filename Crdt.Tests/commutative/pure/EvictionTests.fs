/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.Pure.EvictionTests

open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Crdt.Tests.Convergent.AWORMapTests
open Expecto
open Akkling
open Crdt.Commutative.Pure

let private testListener (sys: ActorSystem) (target: IActorRef<Protocol<int64,int64>>) =
    let dead = TaskCompletionSource<_>()
    let rollback = TaskCompletionSource<_>()
    let rec loop (ctx: Actor<obj>) = actor {
        match! ctx.Receive() with
        | :? IActorRef<Protocol<int64,int64>> as target -> monitor ctx target |> ignore
        | :? Notification<int64,int64> as n ->
            match n with
            | Revoked e -> rollback.SetResult(e.Value)
            | _ -> ()
        | :? Terminated as t -> dead.SetResult(t.ActorRef)
        | _ -> ()
        return! loop ctx
    }
    let actor = spawnAnonymous sys (props loop)
    target <! Subscribe(retype actor)
    actor <! box target
    (dead, rollback)
    

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| ftestList "A pure commutative node eviction" [
    test "evicted node should publish concurrent events and stop" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        
        let a = spawn sys "A" <| props (Counter.props "A")
        let b = spawn sys "B" <| props (Counter.props "B")
        
        Counter.inc 1L a |> wait |> ignore
        Counter.inc 2L b |> wait |> ignore
        
        let (dead, rollback) = testListener sys b
        
        a <! Connect("B", b)
        a <! Evict "B"
        
        
        Expect.equal dead.Task.Result (untyped b) "evicted actor should be stopped"
        Expect.equal rollback.Task.Result 2L "concurrent event should be revoked"
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