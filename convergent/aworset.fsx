/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type AWORSet<'a when 'a: comparison> = AWORSet of add:Map<'a, VTime> * rem:Map<'a, VTime>

[<RequireQualifiedAccess>]
module AWORSet =
    let zero: AWORSet<'a> = AWORSet(Map.empty, Map.empty)

    let value (AWORSet(add, rem)) = 
        rem 
        |> Map.fold(fun acc k vr ->
            match Map.tryFind k acc with
            | Some va when Version.compare va vr = Ord.Lt -> Map.remove k acc
            | _ -> acc) add
        |> Map.toSeq
        |> Seq.map fst
        |> Set.ofSeq

    let add r e (AWORSet(add, rem)) =
        match Map.tryFind e add, Map.tryFind e rem with
        | Some v, _ -> AWORSet(Map.add e (Version.inc r v) add, Map.remove e rem)
        | _, Some v -> AWORSet(Map.add e (Version.inc r v) add, Map.remove e rem)
        | _, _ -> AWORSet(Map.add e (Version.inc r Version.zero) add, rem)

    let rem r e (AWORSet(add, rem)) =
        match Map.tryFind e add, Map.tryFind e rem with
        | Some v, _ -> AWORSet(Map.remove e add, Map.add e (Version.inc r v) rem)
        | _, Some v -> AWORSet(Map.remove e add, Map.add e (Version.inc r v) rem)
        | _, _ -> AWORSet(add, Map.add e (Version.inc r Version.zero) rem)

    let merge (AWORSet(add1, rem1)) (AWORSet(add2, rem2)) =
        let mergeKeys a b =
            b |> Map.fold (fun acc k vb ->
                match Map.tryFind k acc with
                | Some va -> Map.add k (Version.merge va vb) acc
                | None -> Map.add k vb acc ) a
        let addk = mergeKeys add1 add2
        let remk = mergeKeys rem1 rem2
        let add = remk |> Map.fold (fun acc k vr ->
            match Map.tryFind k acc with
            | Some va when Version.compare va vr = Ord.Lt -> Map.remove k acc
            | _ -> acc ) addk
        let rem = addk |> Map.fold (fun acc k va ->
            match Map.tryFind k acc with
            | Some vr when Version.compare va vr <> Ord.Lt -> acc
            | _ -> Map.remove k acc ) remk
        AWORSet(add, rem)