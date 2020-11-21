/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Convergent

open Crdt

type PSet<'a when 'a: comparison> = PSet of add:Set<'a> * rem:Set<'a>

[<RequireQualifiedAccess>]
module PSet =
    let zero: PSet<'a> = PSet(Set.empty, Set.empty)
    // (add, rem) is a single PSet instance
    let value (PSet(add, rem)) = add - rem  
    let add v (PSet(add, rem)) = PSet(Set.add v add, rem)
    let rem v (PSet(add, rem)) = PSet(add, Set.add v rem)
    let merge (PSet(add1, rem1)) (PSet(add2, rem2)) = PSet(add1 + add2, rem1 + rem2)
    
    [<Struct>]
    type Merge<'a when 'a: comparison> = 
        interface IConvergent<PSet<'a>> with
            member __.merge a b = merge a b 