/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

/// Identifier of a replica. In real use cases of RGArrays, string is probably too heavy.
type ReplicaId = string

/// Absolute position identifier.
type Position = int * ReplicaId

type Vertex<'a> =
    { Data: 'a option
      Next: Position option }

/// Available RGArray operations.
type RGAOp<'a> =
  | Insert of value:'a * at:Position * after:Position
  | Remove of at:Position
  member op.At = 
    match op with
    | Insert(_, at, _) | Remove at -> at

type RGArray<'a> = RGA of int * Map<Position, Vertex<'a>>
  with static member Empty: RGArray<'a> = RGA(0, Map.ofList [ (0, ""), { Data = None; Next = None } ])

module RGArray =

  /// A head position of RGArray.
  let head: Position = (0, "")

  /// Returns an empty RGArray.
  let empty(): RGArray<'a> = RGArray<'a>.Empty

  /// Returns an indexed collection represented by the RGArray.
  let value (RGA(_,vertices)): 'a[] =
    let rec foldStep acc fn vertices pos =
      match pos with
      | None   -> acc
      | Some p -> 
        let vertex = Map.find p vertices
        match vertex.Data with
        | None -> foldStep acc fn vertices vertex.Next
        | Some data -> 
          let acc' = fn acc data
          foldStep acc' fn vertices vertex.Next

    let head = (Map.find head vertices).Next
    (foldStep (ResizeArray()) (fun a d -> a.Add d; a) vertices head).ToArray()

  /// Returns an absolute position based on a relative index from a value materialized from RGArray.
  let positionAtIndex index (RGA(_, vertices)) =
    let rec loop vertices i pos =
      let vertex = Map.find pos vertices
      match vertex.Data, vertex.Next with
      | Some _, _ when i = 0 -> Some pos // found
      | Some _, Some n -> loop vertices (i-1) n // move next
      | Some _, None -> None // not found
      | None, None -> None // not found
      | None, Some n -> loop vertices i n // skip
    loop vertices index head

  /// Creates an event that after `apply` will insert value at a given position in the RGArray.
  let insertAfter replica pos value (RGA(max,_)) = Insert(value, (max+1,replica), pos)
  
  /// Creates an event that after `apply` will remove an element at given position from a RGArray.
  let removeAt (pos: Position) (RGA _) = Remove pos

  /// Applies operation (either created locally or from remote replica) to current RGArray.
  let apply op (RGA(seqNr, vertices)): RGArray<'a> =
    let rec update value pos prev vertex vertices =
      match vertex.Next with
      | None ->
        vertices
        |> Map.add prev { vertex with Next = Some pos }
        |> Map.add pos { Data = Some value; Next = None }
      | Some n when n < pos ->
        vertices
        |> Map.add prev { vertex with Next = Some pos }
        |> Map.add pos { Data = Some value; Next = Some n }
      | Some n ->
        let next = Map.find n vertices
        update value pos n next vertices

    match op with
    | Remove at ->
      let vertex = { Map.find at vertices with Data = None }
      RGA(seqNr, Map.add at vertex vertices)
    | Insert(value, at, after) ->
      let prev = Map.find after vertices
      let vertices' = update value at after prev vertices
      let seqNr' = max seqNr (fst at)
      RGA(seqNr', vertices')