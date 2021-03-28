[<AutoOpen>]
module Crdt.Tests.Commutative.Pure.Prolog

open FSharp.Control
open Crdt
open Crdt.Commutative.Pure

let wait = Async.RunSynchronously
