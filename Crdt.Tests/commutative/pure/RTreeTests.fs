/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Commutative.Pure.RTreeTests

open System.Threading
open Expecto
open Akkling
open Crdt.Commutative.Pure

let wait = Async.RunSynchronously

[<Tests>]
let tests = testSequencedGroup "pure commutative" <| testList "A pure commutative Replicated Tree" [
    
    ftest "should not duplicate the same node moved to different positions concurrently" {
        use sys = System.create "sys" <| Configuration.parse "akka.loglevel = INFO"
        let a = spawn sys "A" <| props (RTree.props "A")
        let b = spawn sys "B" <| props (RTree.props "B")
        a <! Connect("B", b)
        b <! Connect("A", a)
        
        let tree = RTree.create "/home" a |> wait 
        
        Thread.Sleep 500
        
        let t1 = RTree.move "/home" "/usr" a |> wait
        let t2 = RTree.move "/home" "/src" b |> wait
        
        Expect.equal t1 (RTree.node "" [RTree.node "usr" []]) ""
        Expect.equal t2 (RTree.node "" [RTree.node "src" []]) ""
        
        Thread.Sleep 500
        
        let t1 = RTree.query a |> wait
        let t2 = RTree.query a |> wait
        
        Expect.equal t1 t2 ""
    }
]
