/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt
   
#load "common.fsx"
#load "protocol.fsx"

open System
open Crdt.Protocol

[<RequireQualifiedAccess>]
module Rga =
    
    /// Virtual index - while physical index of an element in RGA changes as new elements are appended or removed,
    /// a virtual index always stays the same. It allows tracking the item position over time.
    type VPtr = (int * ReplicaId)
        
    type Vertex<'a> = (VPtr * 'a option)
    type Rga<'a> =
        { Sequencer: VPtr
          Vertices: Vertex<'a>[] }
        
    type Command<'a> =
        | Insert of index:int * value:'a
        | RemoveAt of index:int
        
    type Operation<'a> =
        | Inserted of after:VPtr * at:VPtr * value:'a
        | Removed of at:VPtr
        
    /// Checks if given vertex has been tombstoned.
    let inline isTombstone (_, data) = Option.isNone data
        
    /// Maps user-given index (which ignores tombstones) into physical index inside of `vertices` array.
    let private indexWithTombstones index vertices =
        let rec loop offset remaining (vertices: Vertex<'a>[]) =
            if remaining = 0 then offset
            elif isTombstone vertices.[offset] then loop (offset+1) remaining vertices // skip over tombstones
            else loop (offset+1) (remaining-1) vertices
        loop 1 index vertices // skip head as it's always tombstoned (it serves as reference point)
    
    /// Maps user-given VIndex into physical index inside of `vertices` array.
    let private indexOfVPtr ptr vertices =
        let rec loop offset ptr (vertices: Vertex<'a>[]) =
            if ptr = fst vertices.[offset] then offset
            else loop (offset+1) ptr vertices
        loop 0 ptr vertices
        
    /// Recursively checks if the next vertex on the right of a given `offset`
    /// has position higher than `pos` at if so, shift offset to the right.
    /// 
    /// By design, when doing concurrent inserts, we skip over elements on the right
    /// if their Position is higher than Position of inserted element. 
    let rec private shift offset ptr (vertices: Vertex<'a>[]) =
        if offset >= vertices.Length then offset // append at the end
        else
            let (next, _) = vertices.[offset]
            if next < ptr then offset
            else shift (offset+1) ptr vertices // move insertion point to the right
        
    /// Increments given sequence number.
    let inline private nextSeqNr ((i, id): VPtr) : VPtr = (i+1, id)
    
    let private createInserted i value rga = 
      let index = indexWithTombstones i rga.Vertices // start from 1 to skip header vertex
      let prev = fst rga.Vertices.[index-1] // get VPtr of previous element or RGA's head
      let at = nextSeqNr rga.Sequencer
      Inserted(prev, at, value)
      
    let private createRemoved i rga =
      let index = indexWithTombstones i rga.Vertices // start from 1 to skip header vertex
      let at = fst rga.Vertices.[index] // get VPtr of a previous element
      Removed at
    
    let private applyInserted (predecessor: VPtr) (ptr: VPtr) value rga =
      // find index where predecessor vertex can be found
      let predecessorIdx = indexOfVPtr predecessor rga.Vertices
      // adjust index where new vertex is to be inserted
      let insertIdx = shift (predecessorIdx+1) ptr rga.Vertices
      // update RGA to store the highest observed sequence number
      let (seqNr, replicaId) = rga.Sequencer
      let nextSeqNr = (max (fst ptr) seqNr, replicaId)
      let newVertices = Array.insert insertIdx (ptr, Some value) rga.Vertices
      { Sequencer = nextSeqNr; Vertices = newVertices }
      
    let private applyRemoved ptr rga =
      // find index where removed vertex can be found and clear its content to tombstone it
      let index = indexOfVPtr ptr rga.Vertices
      let (at, _) = rga.Vertices.[index]
      { rga with Vertices = Array.replace index (at, None) rga.Vertices }
        
    let private crdt (replicaId: ReplicaId) : Crdt<Rga<'a>, 'a[], Command<'a>, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default = { Sequencer = (0,replicaId); Vertices = [| ((0,""), None) |] }            
            member _.Query rga = rga.Vertices |> Array.choose snd            
            member _.Prepare(rga, cmd) =
                match cmd with
                | Insert(i, value) -> createInserted i value rga
                | RemoveAt(i) -> createRemoved i rga
            member _.Effect(rga, e) =
                match e.Data with
                | Inserted(predecessor, ptr, value) -> applyInserted predecessor ptr value rga
                | Removed(ptr) -> applyRemoved ptr rga
        }
        
    type Endpoint<'a> = Endpoint<Rga<'a>, Command<'a>, Operation<'a>>
        
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    /// Inserts an `item` at given index. To insert at head use 0 index,
    /// to push back to a tail of sequence insert at array length. 
    let insert (index: int) (item: 'a) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (Insert(index, item))
    
    /// Removes item stored at a provided `index`.
    let removeAt (index: int) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (RemoveAt index)
    
    /// Retrieve an array of elements maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'a>) : Async<'a[]> = ref <? Query