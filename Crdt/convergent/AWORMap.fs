/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

namespace Crdt.Convergent

open Crdt

type AWORMap<'k, 'v, 'm when 'k: comparison and 'm: struct and 'm :> IConvergent<'v>> = 
    AWORMap of keys:AWORSet<'k> * entries:Map<'k,'v>

[<RequireQualifiedAccess>]
module AWORMap =

    let zero<'k, 'v, 'm when 'k: comparison and 'm: struct and 'm :> IConvergent<'v>>(): AWORMap<'k, 'v, 'm> = 
        AWORMap(AWORSet.zero, Map.empty)

    let value (AWORMap(_,map)): Map<'k,'v> = map

    let add r (key: 'k) (value: 'v) (m: AWORMap<'k,'v,'m>): AWORMap<'k,'v,'m> =
        let (AWORMap(keys, map)) = m
        let keys' = keys |> AWORSet.add r key
        let map' = map |> Map.add key value
        AWORMap(keys', map')
        
    let rem r key (m: AWORMap<'k,'v,'m>): AWORMap<'k,'v,'m> =
        let (AWORMap(keys, map)) = m
        let keys' = keys |> AWORSet.rem r key
        let map' = map |> Map.remove key
        AWORMap(keys', map')

    let merge (a: AWORMap<'k,'v,'m>) (b: AWORMap<'k,'v,'m>): AWORMap<'k,'v,'m> =
        let (AWORMap(keys1, m1)) = a
        let (AWORMap(keys2, m2)) = b
        let keys3 = AWORSet.merge keys1 keys2
        let m3 =
            keys3
            |> AWORSet.value
            |> Set.fold (fun acc key ->
                match Map.tryFind key m1, Map.tryFind key m2 with
                | Some v1, Some v2 -> 
                    let v3 = Unchecked.defaultof<'m>.merge v1 v2
                    Map.add key v3 acc
                | Some v, None -> Map.add key v acc
                | None, Some v -> Map.add key v acc
                | None, None -> acc 
            ) Map.empty
        AWORMap(keys3, m3)

    [<Struct>]
    type Merge<'k, 'v, 'm when 'k: comparison and 'm: struct and 'm :> IConvergent<'v>> = 
        interface IConvergent<AWORMap<'k, 'v, 'm>> with
            member __.merge a b = merge a b 