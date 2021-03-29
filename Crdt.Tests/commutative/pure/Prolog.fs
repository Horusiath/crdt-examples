/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

[<AutoOpen>]
module Crdt.Tests.Commutative.Pure.Prolog

open FSharp.Control
open Crdt
open Crdt.Commutative.Pure

let wait = Async.RunSynchronously
