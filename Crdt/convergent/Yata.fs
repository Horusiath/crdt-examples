/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Convergent.Yata

open Crdt

type ID = (ReplicaId * uint64)

type Block<'t> =
    { Id: ID                  // unique block identifier
      OriginLeft: Option<ID>  
      OriginRight: Option<ID>
      Value: Option<'t> }
    member this.IsDeleted = Option.isNone this.Value
    
/// A minimal implementation of YATA CRDT. This one lacks any optimizations
/// and is created mostly for educational purposes.
/// 
/// For paper, see: https://www.researchgate.net/publication/310212186_Near_Real-Time_Peer-to-Peer_Shared_Editing_on_Extensible_Data_Types
type Yata<'t> = Block<'t>[]
    
/// Returns zero/default/empty instance of YATA array.
let zero: Yata<'t> = [||]
    
/// Returns a value of YATA array, stripped of all of the metadata, without tombstones.
let value (array: Yata<'t>) : 't[] = 
    array |> Array.choose (fun block -> block.Value)
    
/// Maps used defined `index` into an actual block index, skipping over deleted blocks.
let private findPosition (index: int) (array: Yata<'t>) =
    let mutable i = 0
    let mutable j = 0
    while i < index do
        if not array.[j].IsDeleted then
            i <- i + 1
        j <- j + 1
    j
    
/// Gets last sequence number for a given `replicaId` (0 in no block with given `id` exists).
let private lastSeqNr replicaId (array: Yata<'t>) =
    let rec loop blocks id seqNr i =
        if i >= Array.length blocks then seqNr
        else
            let (id', seqNr') = blocks.[i].Id
            if id' = id then
                loop blocks id (max seqNr seqNr') (i+1)
            else loop blocks id seqNr (i+1)
    loop array replicaId 0UL 0
    
/// Returns index of block identified by given `id` within YATA `array`,
/// or `None` if no such block existed.
let private indexOf (array: Yata<'t>) (id: ID) =
    array |> Array.tryFindIndex (fun b -> b.Id = id)
    
/// Safe indexer function, which returns a block at given index `i`
/// or `None` if index was outside of the bounds of an `array`.
let private getBlock (i: int) (array: Yata<'t>) : Option<Block<'t>> =
    if i < 0 || i >= array.Length then None
    else Some array.[i]
    
/// This function deals with the complexity of determining where to insert
/// a given `block` within YATA `array`, given the circumstances that in the
/// meantime other blocks might have been inserted concurrently.
let rec private findInsertIndex (array: Yata<'t>) block scanning left right dst i =
    let dst = if scanning then dst else i
    if i = right || i = Array.length array then dst
    else
        let o = array.[i]
        let oleft = o.OriginLeft |> Option.bind (indexOf array) |> Option.defaultValue -1
        let oright = o.OriginRight |> Option.bind (indexOf array) |> Option.defaultValue array.Length
        let id1 = fst block.Id
        let id2 = fst o.Id
        
        if oleft < left || (oleft = left && oright = right && id1 <= id2)
        then dst
        else
            let scanning = if oleft = left then id1 <= id2 else scanning
            findInsertIndex array block scanning left right dst (i+1)
    
/// Puts given `block` into an YATA `array` based on the adjacency of its
/// left and right origins. This behavior is shared between `insert` and `merge` functions.
let private integrate (array: Yata<'t>) (block: Block<'t>) : Yata<'t> =
    let (id, seqNr) = block.Id
    let last = lastSeqNr id array
    if last <> seqNr - 1UL
    // since we operate of left/right origins we cannot allow for the gaps between blocks to happen
    then failwithf "operation out of order: tried to insert after (%s,%i): %O" id last block
    else
        let left =
            block.OriginLeft
            |> Option.bind (indexOf array)
            |> Option.defaultValue -1
        let right =
            block.OriginRight
            |> Option.bind (indexOf array)
            |> Option.defaultValue (Array.length array)
        let i = findInsertIndex array block false left right (left+1) (left+1)
        Array.insert i block array
    
/// Inserts a given `value` at provided `index` of an YATA `array`. Insert is performed
/// from the perspective of `replicaId`.
let insert (replicaId: ReplicaId) (index: int) (value: 't) (array: Yata<'t>) : Yata<'t> =
    let i = findPosition index array
    let seqNr = 1UL + lastSeqNr replicaId array
    let left = array |> getBlock (i-1) |> Option.map (fun b -> b.Id)
    let right = array |> getBlock i |> Option.map (fun b -> b.Id)
    let block =
        { Id = (replicaId, seqNr)
          OriginLeft = left
          OriginRight = right
          Value = Some value }
    integrate array block

/// Deletes an element at given `index`. YATA uses tombstones to mark items as deleted,
/// so that they can be later used as reference points (origins) by potential concurrent
/// operations.
let delete (index: int) (blocks: Yata<'t>) : Yata<'t> =
    let i = findPosition index blocks
    let tombstoned = { blocks.[i] with Value = None }
    Array.replace i tombstoned blocks
    
/// Merges two YATA arrays together.
let merge (a: Yata<'t>) (b: Yata<'t>) : Yata<'t> =
    // IDs of the blocks that have been tombstoned
    let tombstones = b |> Array.choose (fun b -> if b.IsDeleted then Some b.Id else None)
    // IDs of blocks already existing in `a`
    let ids = a |> Array.map (fun b -> b.Id)
    let blocks =
        b
        // deduplicate repeating entries
        |> Array.filter (fun block -> not (Array.contains block.Id ids))
        // since integration function requires blocks to be integrated in
        // the order of their sequence numbers we need to sort them out
        |> Array.groupBy (fun block -> fst block.Id)
        |> Array.collect (fun (_, blocks) ->
            blocks |> Array.sortBy (fun block -> snd block.Id)
        )
    blocks
    |> Array.fold integrate a
    |> Array.map (fun block ->
        if not block.IsDeleted && Array.contains block.Id tombstones
        then { block with Value = None } // mark block as deleted
        else block
    )