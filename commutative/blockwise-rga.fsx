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
  | Insert of block:Vec<'a> * at:BlockId * after:BlockId
  | Remove of at:BlockId * length:int

type RGArray<'a> = RGA of maxSeqNr:int * blocks:Map<BlockId, Block<'a>>
  with static member Empty: RGArray<'a> = RGA(0, Map.ofList [ ((0, ""), 0), { Body = Tombstone 0; Next = None; Link = None } ])

module RGArray =

  /// A head position of RGArray.
  let head: BlockId = ((0, ""), 0)

  /// Returns an empty RGArray.
  let empty(): RGArray<'a> = RGArray<'a>.Empty

  /// Returns an indexed collection represented by the RGArray.
  let value (RGA(_,blocks)): 'a[] =
    let rec foldStep acc fn blocks pos =
      match pos with
      | None   -> acc
      | Some p -> 
        let block = Map.find p blocks
        match block.Body with
        | Tombstone _ -> foldStep acc fn blocks block.Next
        | Data data -> 
          let acc' = fn acc data
          foldStep acc' fn blocks block.Next
    

    let head = (Map.find head blocks).Next
    (foldStep (ResizeArray()) (fun a d -> a.AddRange d; a) blocks head).ToArray()

  /// Returns an absolute position based on a relative index from a value materialized from RGArray.
  let blockIdAtIndex (index: int) (RGA(_, blocks)): BlockId option =
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
  let insertAfter (replica: ReplicaId) (pos: BlockId) (block: Vec<'a>) (RGA(max,_)) =
    let blockId: BlockId = ((max+1, replica), 0)  
    Insert(block, blockId, pos)
  
  /// Creates an event that after `apply` will remove an element at given position from a RGArray.
  let removeAt (pos: BlockId) (length: int) (RGA _) = 
    Remove(pos, length) //TODO: if length is longer than block.Body length we need to split this removal into parts

  /// Applies operation (either created locally or from remote replica) to current RGArray.
  let apply op (RGA(seqNr, blocks)): RGArray<'a> =
    /// Check if the offset fits inside a given block, and split it in that case.
    let rec split blocks (at: int) blockId (block: Block<'a>) =
      match block.Body with
      | Data d when at < d.Length -> 
        let (lvec, rvec) = Array.splitAt at d
        let rid: BlockId = (fst blockId, at)
        let right = { Body = Data rvec; Next = block.Next; Link = block.Link }
        let left = { Body = Data lvec; Next = Some rid; Link = Some rid }
        (left, blockId, right, rid)
      | Data d ->
        let blockId' = block.Link.Value
        let block' = Map.find blockId' blocks
        split blocks (at - d.Length) blockId' block'
      | Tombstone len when at < len -> 
        let rid = (fst blockId, at)
        let right = { Body = Tombstone (len - at); Next = block.Next; Link = block.Link }
        let left = { Body = Tombstone at; Next = Some rid; Link = Some rid }
        (left, blockId, right, rid)
      | Tombstone len -> 
        let blockId' = block.Link.Value
        let block' = Map.find blockId' blocks
        split blocks (at - len) blockId' block'

    let rec remove blocks at length =
      match Map.tryFind at blocks with
      | None -> 
        let (position, offset) = at
        let (left, leftId, right, rightId) = split blocks offset at (Map.find (position, 0) blocks)
        let blocks' =
          blocks
          |> Map.add leftId left
          |> Map.add rightId right
        remove blocks' at length
      | Some block ->
        match block.Body with
        | Data d when d.Length = length -> Map.add at { block with Body = Tombstone length} blocks
        | Data d when d.Length > length ->
          let (left, leftId, right, rightId) = split blocks (d.Length - length) at block
          blocks
          |> Map.add leftId { left with Body = Tombstone length }
          |> Map.add rightId right
        | Data d ->
          let remaining = length - d.Length
          let blocks' = Map.add at { block with Body = Tombstone d.Length } blocks
          remove blocks' block.Link.Value remaining
        | Tombstone len when len >= length -> blocks // idempotent do-nothing
        | Tombstone len ->
          remove blocks block.Link.Value (len - length)

    match op with
    | Insert(value, at, after) ->
      match Map.tryFind after blocks with
      | Some prev ->
        let block = { Body = Data value; Next = prev.Next; Link = None }
        let prev' = { prev with Next = Some at }
        let blocks' =
          blocks
          |> Map.add after prev'
          |> Map.add at block
        let atSeqNr = fst (fst at)
        RGA(max seqNr atSeqNr, blocks')
      | None ->
        let (position, offset) = after
        let (left, leftId, right, rightId) = split blocks offset after (Map.find (position,0) blocks)
        let block = { Body = Data value; Next = Some rightId; Link = None }
        let blocks' =
          blocks
          |> Map.add leftId { left with Next = Some at }
          |> Map.add at block
          |> Map.add rightId right
        let atSeqNr = fst (fst at)
        RGA(max seqNr atSeqNr, blocks')
    | Remove(at, length) ->
      let blocks' = remove blocks at length
      RGA(seqNr, blocks')