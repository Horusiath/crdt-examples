/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

open System

#load "common.fsx"
#load "protocol.fsx"

/// Block-wise RGA. It exposes operations for adding/removing multiple elements at once.
[<RequireQualifiedAccess>]
module BWRga =
    
    type Position = (int * ReplicaId)
    [<Struct>]
    type PositionOffset =
        { Position: Position; Offset: int }
        override this.ToString () = sprintf "(%i%s:%i)" (fst this.Position) (snd this.Position) this.Offset
    
    [<Struct>]
    type Content<'a> =
        | Content of content:'a[]
        | Tombstone of skipped:int
        member this.Slice(offset: int) =
            match this with
            | Content data ->
                let left = Array.take offset data
                let right = Array.skip offset data
                (Content left, Content right)
            | Tombstone length ->
                (Tombstone offset, Tombstone (length - offset))
        
    type Block<'a> =
        { Ptr: PositionOffset
          //TODO: in this approach Block contains both user data and CRDT metadata, it's possible
          // however to split these appart and all slicing manipulations can be performed on blocks
          // alone. In this case Query method could return an user data right away with no extra
          // modifications, while the user-content could be stored in optimized structure such as Rope,
          // instead of deeply cloned arrays used here.
          Data: Content<'a> }
        member this.Length =
            match this.Data with
            | Content data -> data.Length
            | Tombstone skipped -> skipped
            
        override this.ToString() =
            sprintf "%O -> %A" this.Ptr this.Data
            
    [<RequireQualifiedAccess>]
    module Block =
        let tombstone (block: Block<'a>) = { block with Data = Tombstone block.Length }
        
        let isTombstone (block: Block<'a>) = match block.Data with Tombstone _ -> true | _ -> false
        
        let split (offset) (block: Block<'a>) =
            if offset = block.Length then (block, None)
            else
                let ptr = block.Ptr
                let (a, b) = block.Data.Slice offset
                let left = { block with Data = a }
                let right = { Ptr = { ptr with Offset = ptr.Offset + offset }; Data = b }
                (left, Some right)
        
    type Rga<'a> =
        { Sequencer: Position
          Blocks: Block<'a>[] }
        
    type Command<'a> =
        | Insert of index:int * 'a[]
        | RemoveAt of index:int * count:int
        
    type Operation<'a> =
        | Inserted of after:PositionOffset * at:Position * value:'a[]
        | Removed of slices:(PositionOffset*int) list
                
    /// Given user-aware index, return an index of a block and position inside of that block,
    /// which matches provided index.
    let private findByIndex idx blocks =
        let rec loop currentIndex consumed (idx: int) (blocks: Block<'a>[]) =
            if idx = consumed then (currentIndex, 0)
            else
                let block = blocks.[currentIndex]
                if Block.isTombstone block then
                    loop (currentIndex+1) consumed idx blocks
                else
                    let remaining = idx - consumed
                    if remaining <= block.Length then
                        // we found the position somewhere in the block
                        (currentIndex, remaining)
                    else
                        // move to the next block with i shortened by current block length
                        loop (currentIndex + 1) (consumed + block.Length) idx blocks
        loop 0 0 idx blocks
                
    let private findByPositionOffset ptr blocks =
        let rec loop idx ptr (blocks: Block<'a>[]) =
            let block = blocks.[idx]
            if block.Ptr.Position = ptr.Position then 
                if block.Ptr.Offset + block.Length >= ptr.Offset  then (idx, ptr.Offset-block.Ptr.Offset)
                else loop (idx+1) ptr blocks
            else loop (idx+1) ptr blocks
        loop 0 ptr blocks
                
    /// Recursively check if the next vertex on the right of a given `offset`
    /// has position higher than `pos` at if so, shift offset to the right.  
    let rec private shift offset pos (blocks: Block<'a>[]) =
        if offset >= blocks.Length then offset // append at the tail
        else
            let next = blocks.[offset].Ptr.Position
            if next < pos then offset
            else shift (offset+1) pos blocks // move insertion point to the right
            
    /// Increments given sequence number.
    let inline private nextSeqNr ((i, id): Position) : Position = (i+1, id)
                
    let private sliceBlocks start count blocks =
        let rec loop acc idx offset remaining (blocks: Block<'a>[]) =
            let block = blocks.[idx]
            let ptr = block.Ptr
            let ptr = { ptr with Offset = ptr.Offset + offset }
            let len = block.Length - offset
            if len > remaining then (ptr, remaining)::acc
            elif len = 0 then loop acc (idx+1) 0 remaining blocks // skip over empty blocks
            else loop ((ptr, len)::acc) (idx+1) 0 (remaining-len) blocks
        let (first, offset) = findByIndex start blocks
        loop [] first offset count blocks |> List.rev
        
    let private filterBlocks slices blocks =
        let rec loop (acc: ResizeArray<Block<'a>>) idx slices (blocks: Block<'a>[]) =
            match slices with
            | [] ->
                for i=idx to blocks.Length-1 do
                    acc.Add blocks.[i] // copy over remaining blocks
                acc.ToArray()
            | (ptr, length)::tail ->
                let block = blocks.[idx]
                if block.Ptr.Position = ptr.Position then // we found valid block
                    let currLen = block.Length
                    if block.Ptr.Offset = ptr.Offset then // the beginning of deleted block was found
                        if currLen = length then // deleted block exactly matches bounds
                            acc.Add (Block.tombstone block)
                            loop acc (idx+1) tail blocks
                        elif currLen < length then // deleted block is longer, delete current one and keep remainder
                            acc.Add (Block.tombstone block)
                            let ptr = { ptr with Offset = ptr.Offset + currLen }
                            loop acc (idx+1) ((ptr, length-currLen)::tail) blocks
                        else // deleted block is shorter, we need to split current block and tombstone left side
                            let (left, Some right) = Block.split length block
                            acc.Add (Block.tombstone left)
                            acc.Add right
                            loop acc (idx+1) tail blocks
                    elif block.Ptr.Offset < ptr.Offset && block.Ptr.Offset + currLen > ptr.Offset then // the deleted block starts inside of a current one
                        let splitPoint = ptr.Offset - block.Ptr.Offset
                        let (left, Some right) = Block.split splitPoint block
                        acc.Add left
                        if length > right.Length then // remainer is longer than right, we need to subtract it and keep around
                            let remainer = length - right.Length
                            acc.Add (Block.tombstone right)
                            let pos = { ptr with Offset = right.Ptr.Offset + right.Length }
                            loop acc (idx+1) ((pos, remainer)::tail) blocks
                        else 
                            let (del, right) = Block.split length right
                            acc.Add (Block.tombstone del)
                            right |> Option.iter acc.Add
                            loop acc (idx+1) tail blocks
                    else    // position ID is correct but offset doesn't fit, we need to move on
                        acc.Add block
                        loop acc (idx+1) slices blocks
                else
                    acc.Add block
                    loop acc (idx+1) slices blocks
        loop (ResizeArray()) 1 slices blocks

    let private crdt (replicaId: ReplicaId) : Crdt<Rga<'a>, 'a[], Command<'a>, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default =
                let head = { Ptr = { Position = (0,""); Offset = 0 }; Data = Tombstone 0 }
                { Sequencer = (0,replicaId); Blocks = [| head |] }            
            member _.Query rga = rga.Blocks |> Array.collect (fun block -> match block.Data with Content data -> data | _ -> [||])        
            member _.Prepare(rga, cmd) =
                match cmd with
                | Insert(idx, slice) ->
                    let (index, offset) = findByIndex idx rga.Blocks
                    let ptr = rga.Blocks.[index].Ptr 
                    let at = nextSeqNr rga.Sequencer
                    Inserted({ ptr with Offset = ptr.Offset+offset }, at, slice)
                | RemoveAt(idx, count) -> 
                    let slices = sliceBlocks idx count rga.Blocks
                    Removed slices
                    
            member _.Effect(rga, e) =
                match e.Data with
                | Inserted(after, at, slice) ->
                    let (index, split) = findByPositionOffset after rga.Blocks
                    let indexAdjusted = shift (index+1) at rga.Blocks
                    let block = rga.Blocks.[index]
                    let newBlock = { Ptr = { Position = at; Offset = 0}; Data = Content slice }
                    let (left, right) = Block.split split block
                    let (seqNr, replicaId) = rga.Sequencer
                    let nextSeqNr = (max (fst at) seqNr, replicaId) 
                    let blocks =
                        rga.Blocks
                        |> Array.replace index left 
                        |> Array.insert indexAdjusted newBlock
                    match right with
                    | Some right ->
                        let blocks = blocks |> Array.insert (indexAdjusted+1) right
                        { Sequencer = nextSeqNr; Blocks = blocks }
                    | None ->
                        { Sequencer = nextSeqNr; Blocks = blocks }
                | Removed(slices) ->
                    let blocks = filterBlocks slices rga.Blocks                    
                    { rga with Blocks = blocks }
        }
        
    type Endpoint<'a> = Endpoint<Rga<'a>, Command<'a>, Operation<'a>>
        
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    let insertRange (index: int) (slice: 'a[]) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (Insert(index, slice))
    
    let removeRange (index: int) (count: int) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (RemoveAt(index, count))
    
    /// Retrieve the current state of the RGA maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'a>) : Async<'a[]> = ref <? Query 