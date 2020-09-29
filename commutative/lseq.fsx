/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt
   
#load "common.fsx"
#load "protocol.fsx"

open System
open Crdt.Protocol

[<RequireQualifiedAccess>]
module LSeq =
    
    [<Struct;CustomComparison;CustomEquality>]
    type VPtr =
        { Sequence: byte[]; Id: ReplicaId }
        override this.ToString() =
            String.Join('.', this.Sequence) + ":" + string this.Id 
        member this.CompareTo(other) =
            // apply lexical comparison of sequence elements
            let len = min this.Sequence.Length other.Sequence.Length
            let mutable i = 0
            let mutable cmp = 0
            while cmp = 0 && i < len do
                cmp <- this.Sequence.[i].CompareTo other.Sequence.[i]
                i <- i + 1
            if cmp = 0 then
                // one of the sequences is subsequence of another one,
                // compare their lengths (cause maybe they're the same)
                // then compare replica ids
                cmp <- this.Sequence.Length - other.Sequence.Length
                if cmp = 0 then this.Id.CompareTo other.Id else cmp
            else cmp
        interface IComparable<VPtr> with member this.CompareTo other = this.CompareTo other
        interface IComparable with member this.CompareTo other = match other with :? VPtr as vptr -> this.CompareTo(vptr)
        interface IEquatable<VPtr> with member this.Equals other = this.CompareTo other = 0
    
    (*
        While this implementation uses ordinary deep-copied array, for practical purposes
        more feasible option seems to be a mutable tree-like collection, 
        using eg. combination of B-link and radix trees.
    *)
    type Vertex<'a> = (VPtr * 'a)
    type LSeq<'a> = Vertex<'a>[]
            
    type Command<'a> =
        | Insert of index:int * value:'a
        | RemoveAt of index:int
        
    type Operation<'a> =
        | Inserted of at:VPtr * value:'a
        | Removed of at:VPtr
        
    /// Binary search for index of `vptr` in an ordered sequence, looking for a place to insert
    /// an element. If `vptr` is the lowest element, 0 will be returned. If it's the highest
    /// one: lseq.Length will be returned.
    let private binarySearch vptr (lseq: LSeq<_>) =
        let mutable i = 0
        let mutable j = lseq.Length
        while i < j do
            let half = (i + j) / 2
            if vptr >= fst lseq.[half] then i <- half + 1
            else j <- half
        i
    
    /// Generates a byte sequence that - ordered lexically - would fit between `lo` and `hi`.
    let private generateSeq (lo: byte[]) (hi: byte[]) =
        let rec loop (acc: ResizeArray<byte>) i (lo: byte[]) (hi: byte[])  =
            let min = if i >= lo.Length then 0uy else lo.[i]
            let max = if i >= hi.Length then 255uy else hi.[i]
            if min + 1uy < max then
                acc.Add (min + 1uy)
                acc.ToArray()
            else
                acc.Add min
                loop acc (i+1) lo hi
        loop (ResizeArray (min lo.Length hi.Length)) 0 lo hi
    
    let private crdt (replicaId: ReplicaId) : Crdt<LSeq<'a>, 'a[], Command<'a>, Operation<'a>> =
        { new Crdt<_,_,_,_> with
            member _.Default = [||]     
            member _.Query lseq = lseq |> Array.map snd
            member _.Prepare(lseq, cmd) =
                match cmd with
                | Insert(i, value) ->
                    let left = if i = 0 then [||] else (fst lseq.[i-1]).Sequence  
                    let right = if i = lseq.Length then [||] else (fst lseq.[i]).Sequence
                    let ptr = { Sequence = generateSeq left right; Id = replicaId }
                    Inserted(ptr, value)
                | RemoveAt(i) -> Removed(fst lseq.[i])
            member _.Effect(lseq, e) =
                match e.Data with
                | Inserted(ptr, value) ->
                    let idx = binarySearch ptr lseq
                    Array.insert idx (ptr, value) lseq
                | Removed(ptr) -> 
                    let idx = binarySearch ptr lseq
                    Array.removeAt idx lseq
        }
        
    type Endpoint<'a> = Endpoint<LSeq<'a>, Command<'a>, Operation<'a>>
        
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    /// Inserts an `item` at given index. To insert at head use 0 index,
    /// to push back to a tail of sequence insert at array length. 
    let insert (index: int) (item: 'a) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (Insert(index, item))
    
    /// Removes item stored at a provided `index`.
    let removeAt (index: int) (ref: Endpoint<'a>) : Async<'a[]> = ref <? Command (RemoveAt index)
    
    /// Retrieve an array of elements maintained by the given `ref` endpoint. 
    let query (ref: Endpoint<'a>) : Async<'a[]> = ref <? Query