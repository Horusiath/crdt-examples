/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

(*
  NOTE: this is a blockwise immutable implementation of Replicated Growable Array. 
        It's meant mostly for educational purposes. It's not optimized for speed.
        Also it doesn't support tombstone prunning atm. Changing this implementation
        into mutable one with direct links between data blocks would bring a big
        performance gain. Another optimization would be about using B-Tree index for
        efficient mapping from index in value array to actual block.
*)

/// Absolute position identifier of a given block sequence.
type Position = int * ReplicaId

/// Absolute position identifier of an elements inside of a given block sequence.
type BlockId = Position * int

/// Alias for a segment used - potentially could be Memory<>, ArraySegment<> or ByteBuffer.
type Vec<'a> = 'a[] 

type Body<'a> =
    | Data of Vec<'a>
    | Tombstone of length:int
    member b.Length =
      match b with
      | Data d -> d.Length
      | Tombstone l -> l

/// Block describes a committed segment of values or a tombstone of given length. The
/// `Next` field is an ID of the next block in indexed sequence being the RGA result.
/// In case of splitting the block for the purpose of insert-between operation on the
/// continuous segment, a `Link` is used to remember the blocks that have been a single
/// one before the split.
type Block<'a> =
    { Body: Body<'a>
      Next: BlockId option
      Link: BlockId option }

/// Available RGArray operations.
type RGAOp<'a> =
  | Insert of segment:Vec<'a> * at:BlockId * after:BlockId
  | Remove of at:BlockId * length:int
  member op.At =
    match op with Insert(_, at, _) | Remove(at, _) -> at

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
    let rec loop blocks remaining id =
      let vertex = Map.find id blocks
      match vertex.Body, vertex.Next with
      | Data d, _ when remaining < d.Length -> Some (fst id, remaining + snd id) // found
      | Data d, Some n -> loop blocks (remaining-d.Length) n // move next
      | Data _, None -> None // not found
      | Tombstone _, None -> None // not found
      | Tombstone _, Some n -> loop blocks remaining n // move over tombstone (don't count its length)
    loop blocks index head

  /// Creates an event that after `apply` will insert value at a given position in the RGArray.
  let insertAfter (replica: ReplicaId) (pos: BlockId) (block: Vec<'a>) (RGA(max,_)) =
    let blockId: BlockId = ((max+1, replica), 0)  
    Insert(block, blockId, pos)
  
  /// Finds a first block containing a position specified by a given BlockId (Position of head + offset).
  let private findContainingBlock (blockId: BlockId) blocks =
    let rec loop blocks offset blockId =
      let block = Map.find blockId blocks
      if block.Body.Length > offset 
      then ((fst blockId, offset), block)
      else loop blocks (offset - block.Body.Length) (block.Next.Value)

    match Map.tryFind blockId blocks with
    | Some block -> (blockId, block)
    | None ->
      let (position, offset) = blockId
      loop blocks offset (position, 0)

  /// Creates an event that after `apply` will remove a given number of elements starting at given position.
  /// 
  /// In case when removal will need to affect multiple blocks to satisfy requested lenght, a multiple events
  /// will be produced - one per each affected block. This is necessary to correctly execute removal operation
  /// in face of concurrent updates.
  let rec removeAt (at: BlockId) (length: int) rga =
    let (RGA (_, blocks)) = rga
    match Map.tryFind at blocks with
    | Some b when b.Body.Length >= length -> [Remove(at, length)]
    | Some b ->
      let event = Remove(at, b.Body.Length)
      event::(removeAt b.Next.Value (length - b.Body.Length) rga)
    | None -> 
      let ((pos, offset), block) = findContainingBlock at blocks
      if block.Body.Length - offset >= length 
      then [Remove(at, length)]
      else 
        let remaining = length - (block.Body.Length - offset)
        Remove(at, length - remaining)::(removeAt block.Next.Value remaining rga)

  /// Applies operation (either created locally or from remote replica) to current RGArray.
  let apply op (RGA(seqNr, blocks)): RGArray<'a> =
    /// Check if the offset fits inside a given block, and split it in that case.
    let rec split blocks (at: int) blockId (block: Block<'a>) =
      match block.Body with
      | Data d when at < d.Length -> 
        // We found a block, which needs to be split.
        let (lvec, rvec) = Array.splitAt (at+1) d
        let rid: BlockId = (fst blockId, (snd blockId) + at+1)
        let right = { Body = Data rvec; Next = block.Next; Link = block.Link }
        let left = { Body = Data lvec; Next = Some rid; Link = Some rid }
        (left, blockId, right, rid)
      | Tombstone len when at < len -> 
        // We found a block to split, but it's a tombstone. Just split it into two tombstones.
        let rid = (fst blockId, at)
        let right = { Body = Tombstone (len - at); Next = block.Next; Link = block.Link }
        let left = { Body = Tombstone at; Next = Some rid; Link = Some rid }
        (left, blockId, right, rid)
      | _ ->
        // This is not a block to be split - subtract the block's length from expected `at` offset
        // and continue with the next linked block.
        let blockId' = block.Link.Value
        let block' = Map.find blockId' blocks
        split blocks (at - block.Body.Length) blockId' block'

    /// Marks block at given position as tombstone. If necessary, block may be split into
    /// 2 or 3 smaller ones to separate the block which is supposed to be tombstoned.  
    let rec remove blocks at length =
      match Map.tryFind at blocks with
      | None -> 
        // Block was not found directly, so it must be a position somewhere in the middle of
        // another block. We can find it by starting at first block in sequence (that's why `at'`
        // has offset set to 0) and spliting it in two.
        let (position, offset) = at
        let at' = (position, 0)
        let (left, leftId, right, rightId) = split blocks (offset-1) at' (Map.find at' blocks)
        let blocks' =
          blocks
          |> Map.add leftId left
          |> Map.add rightId right
        remove blocks' at length
      | Some block ->
        match block.Body with
        | Data d when d.Length = length -> Map.add at { block with Body = Tombstone length} blocks
        | Data d when d.Length > length ->
          // Block was found but it's longer than requested removal length. We need to split 
          // it in two and then tombstone the left part.
          let (left, leftId, right, rightId) = split blocks (length-1) at block
          blocks
          |> Map.add leftId { left with Body = Tombstone length }
          |> Map.add rightId right
        | Data d ->
          // Block was found, but it's shorter than requested removal length - it means that it was
          // subject to split due to insert/remove operation that happened concurrently. Tomstone 
          // entire block, then proceed through `Link` to the next block, to tombstone a remaining part. 
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
        // Current insert fits perfectly after existing block. We can simply insert current block
        // between the `prev` and `prev`'s successor.
        let block = { Body = Data value; Next = prev.Next; Link = None }
        let prev' = { prev with Next = Some at }
        let blocks' =
          blocks
          |> Map.add after prev'
          |> Map.add at block
        let atSeqNr = fst (fst at)
        RGA(max seqNr atSeqNr, blocks')
      | None ->
        // Current insert is happening somewhere in the middle of existing block. In that case,
        // we need to find that block and split it in two, then insert current block between
        // the splits.
        let (position, offset) = after
        let at' = (position, 0)
        let (left, leftId, right, rightId) = split blocks offset at' (Map.find at' blocks)
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