/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

/// Absolute position identifier of a given block sequence.
type Position = int * ReplicaId

/// Absolute position identifier of an elements inside of a given block sequence.
type BlockId = Position * int

/// Alias for a segment used - potentially could be Memory<>, ArraySegment<> or ByteBuffer.
type Vec<'a> = 'a[]

type Body<'a> =
    | Data of Vec<'a>
    | Tombstone of length:int

type Block<'a> =
    { Body: Body<'a>
      Next: BlockId option
      Link: BlockId option }

/// Available RGArray operations.
type RGAOp<'a> =
  | Insert of value:'a * at:BlockId * after:BlockId
  | Remove of at:BlockId * length:int
  | Prune of VTime

type RGArray<'a> = RGA of maxSeqNr:int * version:MClock * blocks:Map<BlockId, Block<'a>>
  with static member Empty: RGArray<'a> = RGA(0, Map.empty, Map.ofList [ ((0, ""), 0), { Body = Tombstone 0; Next = None; Link = None } ])

module RGArray =

  /// A head position of RGArray.
  let head: BlockId = ((0, ""), 0)

  /// Returns an empty RGArray.
  let empty(): RGArray<'a> = RGArray<'a>.Empty

  /// Returns an indexed collection represented by the RGArray.
  let value (RGA(_,_,blocks)): 'a[] =
    let rec foldStep acc fn blocks pos =
      match pos with
      | None   -> acc
      | Some p -> 
        let vertex = Map.find p blocks
        match vertex.Body with
        | Tombstone _ -> foldStep acc fn blocks vertex.Next
        | Data data -> 
          let acc' = fn acc data
          foldStep acc' fn blocks vertex.Next

    let head = (Map.find head blocks).Next
    (foldStep (ResizeArray()) (fun a d -> a.AddRange d; a) blocks head).ToArray()

  /// Returns an absolute position based on a relative index from a value materialized from RGArray.
  let positionAtIndex index (RGA(_,_, blocks)) =
    let rec loop blocks i pos =
      let vertex = Map.find pos blocks
      match vertex.Body, vertex.Next with
      | Data d, _ when i < d.Length -> Some (fst pos, i) // found
      | Data d, Some n -> loop blocks (i-d.Length) n // move next
      | Data _, None -> None // not found
      | Tombstone _, None -> None // not found
      | Tombstone _, Some n -> loop blocks i n // skip
    loop blocks index head

  /// Creates an event that after `apply` will insert value at a given position in the RGArray.
  let insertAfter replica pos value (RGA(max,_,_)) = ??? // Insert(value, (max+1,replica), pos)
  
  /// Creates an event that after `apply` will remove an element at given position from a RGArray.
  let removeAt (pos: BlockId) (length: int) (RGA _) = ??? // Remove pos

  let prune (RGA(_, version, _)) =
      let seenByAll = MVersion.min version
      Prune seenByAll

  /// Applies operation (either created locally or from remote replica) to current RGArray.
  let apply op (RGA(seqNr, version, blocks)): RGArray<'a> =
    // let rec update value pos prev vertex blocks =
    //   match vertex.Next with
    //   | None ->
    //     blocks
    //     |> Map.add prev { vertex with Next = Some pos }
    //     |> Map.add pos { Data = Some value; Next = None }
    //   | Some n when n < pos ->
    //     blocks
    //     |> Map.add prev { vertex with Next = Some pos }
    //     |> Map.add pos { Data = Some value; Next = Some n }
    //   | Some n ->
    //     let next = Map.find n blocks
    //     update value pos n next blocks

    match op with
    | Remove(at, length) ->
      // let vertex = { Map.find at blocks with Data = None }
      // RGA(seqNr, Map.add at vertex blocks)
    | Insert(value, at, after) ->
      // let prev = Map.find after blocks
      // let blocks' = update value at after prev blocks
      // let seqNr' = max seqNr (fst at)
      // RGA(seqNr', blocks')
    | Prune before ->
      let blocks' = 
        Map.fold (fun acc pos block -> 
          match block.Body with
          | Tombstone v when Version.compare v before = Ord.Lt -> Map.remove pos acc
          | _ -> acc) blocks blocks
        RGA(seqNr, version, blocks')