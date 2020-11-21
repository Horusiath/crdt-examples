/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative

open System
open Crdt
open Akkling

/// Block-wise RGA. It exposes operations for adding/removing multiple elements at once.
[<RequireQualifiedAccess>]
module BWRga =
    
    type VPtr = (int * ReplicaId)
    [<Struct>]
    type VPtrOff =
        { Ptr: VPtr; Offset: int }
        override this.ToString () = sprintf "(%i%s:%i)" (fst this.Ptr) (snd this.Ptr) this.Offset
    
    [<Struct>]
    type Content<'a> =
        | Tombstone of skipped:int
        | Content of content:'a[]
        member this.Slice(offset: int) =
            match this with
            | Content data ->
                let left = Array.take offset data
                let right = Array.skip offset data
                (Content left, Content right)
            | Tombstone length ->
                (Tombstone offset, Tombstone (length - offset))
        
    type Block<'a> =
        { PtrOffset: VPtrOff
          //TODO: in this approach Block contains both user data and CRDT metadata, it's possible
          // however to split these apart and all slicing manipulations can be performed on blocks
          // alone. In this case Query method could return an user data right away with no extra
          // modifications, while the user-content could be stored in optimized structure such as Rope,
          // instead of deeply cloned arrays used here.
          Data: Content<'a> }
        member this.Length =
            match this.Data with
            | Content data -> data.Length
            | Tombstone skipped -> skipped
            
        override this.ToString() =
            sprintf "%O -> %A" this.PtrOffset this.Data
            
    [<RequireQualifiedAccess>]
    module Block =
        /// Tombstones the `block`, returning new immutable block version.
        let inline tombstone (block: Block<'a>) = { block with Data = Tombstone block.Length }
        
        /// Checks if current block is marked as tombstoned.
        let isTombstone (block: Block<'a>) = match block.Data with Tombstone _ -> true | _ -> false
        
        /// Checks if a given `offset` may be found within current `block`. This means
        /// that offset is higher or equal to block's own offset and is no further than
        /// the block's length. 
        let inline containsOffset (offset: int) (block: Block<'a>) =
            let beginning = block.PtrOffset.Offset 
            beginning <= offset && beginning + block.Length >= offset
        
        /// Splits current block at a given index. Returns pair of blocks splitted that way
        /// with their vptr+offsets adjusted.
        let split (index) (block: Block<'a>) =
            if index >= block.Length then (block, None) // check if it's possible to slice the block
            else
                let ptr = block.PtrOffset
                let (a, b) = block.Data.Slice index // slice the data in two
                let left = { block with Data = a }   // re-assign left slice to left block
                // move the offset for the right block pointer to reflect right block offset start position
                let rightPtr = { ptr with Offset = ptr.Offset + index } 
                let right = { PtrOffset = rightPtr; Data = b }
                (left, Some right)
        
    type Rga<'a> =
        { Sequencer: VPtr
          Blocks: Block<'a>[] }
        
    type Command<'a> =
        | Insert of index:int * 'a[]
        | RemoveAt of index:int * count:int
        
    type Operation<'a> =
        | Inserted of after:VPtrOff * at:VPtr * value:'a[]
        | Removed of slices:(VPtrOff*int) list
                
    /// Given user-aware index, return an index of a block and position inside of that block,
    /// which matches provided index.
    let private findByIndex idx blocks =
        let rec loop currentIndex consumed (idx: int) (blocks: Block<'a>[]) =
            if idx = consumed then (currentIndex, 0)
            else
                let block = blocks.[currentIndex]
                if Block.isTombstone block then
                    loop (currentIndex+1) consumed idx blocks // skip over tombstoned blocks
                else
                    let remaining = idx - consumed
                    if remaining <= block.Length then
                        // we found the position somewhere within the block
                        (currentIndex, remaining)
                    else
                        // move to the next block with i shortened by current block length
                        loop (currentIndex + 1) (consumed + block.Length) idx blocks
        loop 0 0 idx blocks
                
    /// Searches among the blocks to find the block that matches provided virtual pointer
    /// and offset. Returns pair: index off block found within `blocks` and index within
    /// that block that points to a position provided in p.Offset.
    let private findByPositionOffset (p: VPtrOff) blocks =
        let rec loop idx p (blocks: Block<'a>[]) =
            let block = blocks.[idx]
            if block.PtrOffset.Ptr = p.Ptr then
                if block |> Block.containsOffset p.Offset
                then
                    // we found the correct block, now we need an index within that block
                    let blockIdx = p.Offset - block.PtrOffset.Offset
                    (idx, blockIdx)
                else loop (idx+1) p blocks
            else loop (idx+1) p blocks
        loop 0 p blocks
                
    /// Recursively check if the next vertex on the right of a given `offset`
    /// has position higher than `pos` at if so, shift offset to the right.  
    let rec private shift offset ptr (blocks: Block<'a>[]) =
        if offset >= blocks.Length then offset // append at the tail
        else
            let next = blocks.[offset].PtrOffset.Ptr
            if next < ptr then offset
            else shift (offset+1) ptr blocks // move insertion point to the right
            
    /// Increments given sequence number.
    let inline private nextSeqNr ((i, id): VPtr) : VPtr = (i+1, id)
      
    /// Handles removal of range of elements. Input parameters `index` and `count` are
    /// user-relative and determine starting position and number of elements to remove. 
    /// 
    /// Returns event with list pointer-offsets of blocks affected by removal and number
    /// of elements removed in each block, since internally this continuous range may be
    /// represented by multiple blocks, and may start or end inside of a block.
    let private handleRemove index count blocks =
        let rec loop acc idx offset remaining (blocks: Block<'a>[]) =
            let block = blocks.[idx]
            let ptr = block.PtrOffset
            let ptr = { ptr with Offset = ptr.Offset + offset }
            let len = block.Length - offset
            if len > remaining then (ptr, remaining)::acc
            elif len = 0 then loop acc (idx+1) 0 remaining blocks // skip over empty blocks
            else loop ((ptr, len)::acc) (idx+1) 0 (remaining-len) blocks
        /// find actual index of a block containing the `index` parameter
        /// and an offset within that block, that exactly matches the index parameter
        let (first, offset) = findByIndex index blocks
        loop [] first offset count blocks |> List.rev |> Removed
        
    /// Tombstones the blocks accordingly to the hints described by `handleRemove` function.
    /// It takes a list of `slices` which describe the identifiers of specific blocks to be
    /// affected and a number of elements to be tombstoned within each block.
    let private applyRemoved slices blocks =
        /// Takes a slice and shortens it by moving its pointer offset
        /// and reducing the length by a given value.
        let inline shorten by (ptr, length) =
            let ptr = { ptr with Offset = ptr.Offset + by }
            let remaining = length - by
            (ptr, remaining)
            
        let rec loop (acc: ResizeArray<Block<'a>>) idx slices (blocks: Block<'a>[]) =
            match slices with
            | [] ->
                for i=idx to blocks.Length-1 do
                    acc.Add blocks.[i] // copy over remaining blocks
                acc.ToArray()
            | (ptr, length)::tail ->
                let block = blocks.[idx]
                if block.PtrOffset.Ptr = ptr.Ptr then // we found valid block
                    let currLen = block.Length
                    if block.PtrOffset.Offset = ptr.Offset then // the beginning of tombstoned block was found
                        if currLen < length then // current block is shorter than expected, tombstone it and keep remainder
                            acc.Add (Block.tombstone block)
                            // we replace current slice with the one having updated offset and remaining length
                            let remaining = shorten currLen (ptr, length)
                            loop acc (idx+1) (remaining::tail) blocks
                        else // current block has exact length is longer than expected, we need to split it and tombstone left side
                            let (left, right) = Block.split length block
                            acc.Add (Block.tombstone left)
                            right |> Option.iter acc.Add // if block has exact length right side of a split will be None
                            loop acc (idx+1) tail blocks
                    elif block |> Block.containsOffset ptr.Offset then // the tombstoned slice starts inside of a current block
                        // split block, copy over left part
                        let splitIndex = ptr.Offset - block.PtrOffset.Offset
                        let (left, Some right) = Block.split splitIndex block
                        acc.Add left
                        if length > right.Length then // tombstone remainer length is longer than size of a right block
                            acc.Add (Block.tombstone right)
                            // update slice to contain remaining length to be tombstoned and repeat
                            let remaining = shorten right.Length (right.PtrOffset, length)
                            //let remainer = length - right.Length
                            //let pos = { ptr with Offset = right.PtrOffset.Offset + right.Length }
                            loop acc (idx+1) (remaining::tail) blocks
                        else 
                            let (del, right) = Block.split length right
                            acc.Add (Block.tombstone del)
                            right |> Option.iter acc.Add
                            loop acc (idx+1) tail blocks
                    else    // position ID is correct but offset doesn't fit, we need to move on
                        acc.Add block
                        loop acc (idx+1) slices blocks
                else // this is not a block we're looking for, just copy it over
                    acc.Add block
                    loop acc (idx+1) slices blocks
        loop (ResizeArray()) 1 slices blocks
        
    let private handleInsert idx items rga =
        let (index, offset) = findByIndex idx rga.Blocks
        let ptr = rga.Blocks.[index].PtrOffset 
        let at = nextSeqNr rga.Sequencer
        Inserted({ ptr with Offset = ptr.Offset+offset }, at, items)
        
    /// Inserts a new fresh block containing given `items` - identified by `ptr` - right after
    /// the block identifier by `predecessor`. It's possible that predecessor points in the
    /// middle of another block - in that case it will be split in two and a new block will be
    /// inserted in between splatted parts.
    let private applyInserted predecessor ptr items rga =
        let (index, blockIndex) = findByPositionOffset predecessor rga.Blocks
        // in case of concurrent insert of two or more blocks we need to check which of them
        // should be shifted to the right and which should stay, and adjust insertion index
        let indexAdjusted = shift (index+1) ptr rga.Blocks
        let block = rga.Blocks.[index]
        // since we're about to insert a new block, its offset will always start at 0
        // (it can only be changed, when the block is subject to slicing)
        let newBlock = { PtrOffset = { Ptr = ptr; Offset = 0}; Data = Content items }
        let (left, right) = Block.split blockIndex block
        let blocks =
            rga.Blocks
            |> Array.replace index left  // replace predecessor block if split occurred
            |> Array.insert indexAdjusted newBlock // insert new block
        // if split occurred we also need to insert right block piece back into an array
        let blocks = right |> Option.fold (fun blocks b -> Array.insert (indexAdjusted+1) b blocks) blocks
        // update sequencer
        let (seqNr, replicaId) = rga.Sequencer
        let nextSeqNr = (max (fst ptr) seqNr, replicaId) 
        { Sequencer = nextSeqNr; Blocks = blocks }

    let private crdt (replicaId: ReplicaId) : Crdt<Rga<'a>, 'a[], Command<'a>, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default =
                let head = { PtrOffset = { Ptr = (0,""); Offset = 0 }; Data = Tombstone 0 }
                { Sequencer = (0,replicaId); Blocks = [| head |] }            
            member _.Query rga = rga.Blocks |> Array.collect (fun block -> match block.Data with Content data -> data | _ -> [||])        
            member _.Prepare(rga, cmd) =
                match cmd with
                | Insert(idx, items) -> handleInsert idx items rga
                | RemoveAt(idx, count) -> handleRemove idx count rga.Blocks
                    
            member _.Effect(rga, e) =
                match e.Data with
                | Inserted(after, at, items) -> applyInserted after at items rga
                | Removed(slices) ->
                    let blocks = applyRemoved slices rga.Blocks                    
                    { rga with Blocks = blocks }
        }
        
    type Endpoint<'a> = Endpoint<Rga<'a>, Command<'a>, Operation<'a>>
        
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    let insertRange (index: int) (slice: 'a[]) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (Insert(index, slice))
    
    let removeRange (index: int) (count: int) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (RemoveAt(index, count))
    
    /// Retrieve the current state of the RGA maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'a>) : Async<'a[]> = ref <? Query 