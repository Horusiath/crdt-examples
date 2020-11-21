/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative

open System.Collections.Generic
open System.Text
open Crdt
open Akkling

module Json =
    
    /// Primitive type that can be assigned to a register.
    type Primitive =
        | Null
        | Int of int
        | Float of float
        | String of string
        | Bool of bool
        static member ($) (_: Primitive, x: int) = Int x
        static member ($) (_: Primitive, x: float) = Float x
        static member ($) (_: Primitive, x: string) = String x
        static member ($) (_: Primitive, x: bool) = Bool x
        override this.ToString() =
            match this with
            | Null -> "null"
            | String s -> "\"" + s + "\""
            | Int i -> string i
            | Float f -> string f
            | Bool true -> "true"
            | Bool false -> "false"
            
    [<RequireQualifiedAccess>]
    type Json =
        /// An entry which could be resolved into a single primitive type.
        | Value of Primitive
        | Concurrent of Json list
        | Array of Json[]
        | Obj of Map<string, Json>
        override this.ToString() =
            let rec stringify (sb: StringBuilder) (json: Json) =
                match json with
                | Value x -> sb.Append(string x) |> ignore
                | Array items ->
                    sb.Append "[" |> ignore
                    let mutable e = downcast items.GetEnumerator()
                    if e.MoveNext() then stringify sb (downcast e.Current)
                    while e.MoveNext() do
                        sb.Append ", " |> ignore
                        stringify sb (downcast e.Current)
                    sb.Append "]" |> ignore
                | Obj map ->
                    sb.Append "{" |> ignore
                    let mutable e = (Map.toSeq map).GetEnumerator()
                    if e.MoveNext() then
                        let (key, value) = e.Current
                        sb.Append('"').Append(key).Append("\": ") |> ignore
                        stringify sb value
                    while e.MoveNext() do
                        sb.Append(", ") |> ignore
                        let (key, value) = e.Current
                        sb.Append('"').Append(key).Append("\": ") |> ignore
                        stringify sb value
                    sb.Append "}" |> ignore
                | Concurrent values ->
                    sb.Append "(" |> ignore
                    let mutable e = (List.toSeq values).GetEnumerator()
                    if e.MoveNext() then stringify sb e.Current
                    while e.MoveNext() do
                        sb.Append " | " |> ignore
                        stringify sb e.Current
                    sb.Append ")" |> ignore
                    
            let sb = StringBuilder()
            stringify sb this
            sb.ToString()
    
    type VPtr = (int64 * ReplicaId)
    
    type Vertex<'a> = (VPtr * 'a)
         
    [<Struct>]   
    type Entry<'a> =
        { Value: 'a option
          Timestamp: VTime }
        
    and Node =
        | Values of Primitive
        | Array of Vertex<Doc>[]
        | Object of Map<string, Doc>
        
    and Doc = Entry<Node> list
        
    [<RequireQualifiedAccess>]
    module Entry =
        
        let inline create timestamp value = { Value = Some value; Timestamp = timestamp }
                    
        let inline update (timestamp: VTime) value (entry: Entry<_>) =
            { Value = Some value; Timestamp = Version.merge timestamp entry.Timestamp }
        
    [<RequireQualifiedAccess>]
    module Doc =
        
        let empty : Doc = []
        
        let getValues (doc: Doc) =
            doc |> List.choose (fun e -> match e.Value with Some(Values p) -> Some p | _ -> None)
            
        let getObject (doc: Doc) =
            doc
            |> List.choose (fun e -> match e.Value with Some(Object o) -> Some o | _ -> None)
            |> List.tryHead
            
        let getArray (doc: Doc) =
            doc
            |> List.choose (fun e -> match e.Value with Some(Array o) -> Some o | _ -> None)
            |> List.tryHead
                    
        let assign (timestamp: VTime) (value: Primitive) (doc: Doc) : Doc =
            let concurrent =
                doc
                |> List.choose (fun e ->
                    match e.Value with
                    | Some(Values x) when Version.compare e.Timestamp timestamp <= Ord.Eq -> None
                    | Some(Values x) -> Some e
                    | Some other when Version.compare e.Timestamp timestamp <= Ord.Eq -> Some { e with Value = None }
                    | _ -> Some e
                )
            { Value = Some(Values value); Timestamp = timestamp }::concurrent
                        
        let rec tombstone (timestamps: VTime list) (doc: Doc) : Doc =
            doc
            |> List.choose (fun e ->
                match timestamps |> List.tryFind (fun ts -> Version.compare e.Timestamp ts <= Ord.Eq) with
                | Some timestamp ->
                    match e.Value with
                    | Some (Values _) -> None
                    | Some (Array a)  ->
                        let b = a |> Array.map (fun (vptr, doc) -> (vptr, tombstone timestamps doc))
                        Some { Timestamp = timestamp; Value = Some (Array b) }
                    | Some (Object a) -> 
                        let b = a |> Map.map (fun key doc -> tombstone timestamps doc)
                        Some { Timestamp = timestamp; Value = Some (Object b) }
                    | None -> Some { e with Timestamp = timestamp }
                | None ->
                    // not found, try deeper
                    match e.Value with
                    | Some (Array a)  ->
                        let b = a |> Array.map (fun (vptr, doc) -> (vptr, tombstone timestamps doc))
                        Some { e with Value = Some (Array b) }
                    | Some (Object a) -> 
                        let b = a |> Map.map (fun key doc -> tombstone timestamps doc)
                        Some { e with Value = Some (Object b) }
                    | other -> Some e)
            
        let set (modify: Node -> Node option) (timestamp: VTime) (doc: Doc) : Doc =
            doc
            |> List.choose (fun e ->
                match e.Value with
                | None -> Some { e with Timestamp = Version.max e.Timestamp timestamp }
                | Some (Values _) when Version.compare e.Timestamp timestamp <= Ord.Eq-> None
                | Some other -> modify other |> Option.map (fun o -> { e with Timestamp = Version.max e.Timestamp timestamp; Value = Some o }))
            
        let isTombstoned (doc: Doc) = doc |> List.forall (fun e -> Option.isNone e.Value)

    type Command =
        | Assign   of Primitive
        | Update   of key:string * Command
        | UpdateAt of index:int * Command
        | InsertAt of index:int * Command
        | Remove
        
    type Operation =
        | Updated   of key:string * Operation
        | InsertedAt of predecessor:VPtr option * current:VPtr * Operation
        | UpdatedAt of VPtr * Operation
        | Assigned  of Primitive
        | Removed   of VTime list
        
    let rec valueOf (doc: Doc) : Json option =
        let out =
            doc
            |> List.choose (fun e ->
                match e.Value with
                | None -> None
                | Some (Values v) -> Some (Json.Value v)
                | Some (Array a)  ->
                    let b = a |> Array.choose (fun (_, doc) -> valueOf doc)
                    if Array.isEmpty b then None else Some (Json.Array b)
                | Some (Object a) ->
                    let b =
                        a
                        |> Seq.choose (fun e -> valueOf e.Value |> Option.map (fun v -> (e.Key, v)))
                        |> Map.ofSeq
                    if Map.isEmpty b then None else Some (Json.Obj b))

        match out with
        | []  -> None
        | [x] -> Some (x)
        | conflicts -> Some (Json.Concurrent conflicts)
        
    /// Maps user-given index (which ignores tombstones) into physical index inside of `vertices` array.
    let private indexWithTombstones index vertices =
        let rec loop offset remaining (vertices: Vertex<Doc>[]) =
            if remaining = 0 then offset
            elif Doc.isTombstoned (snd vertices.[offset]) then loop (offset+1) remaining vertices // skip over tombstones
            else loop (offset+1) (remaining-1) vertices
        loop 0 index vertices // skip head as it's always tombstoned (it serves as reference point)
    
    /// Maps user-given VIndex into physical index inside of `vertices` array.
    let private indexOfVPtr ptr vertices =
        let rec loop offset ptr (vertices: Vertex<'a>[]) =
            if ptr = fst vertices.[offset] then offset
            else loop (offset+1) ptr vertices
        loop 0 ptr vertices
        
    let nextSeqNr (vertices: Vertex<'a>[]) =
        if Array.isEmpty vertices then 1L
        else
            vertices
            |> Array.toSeq
            |> Seq.map (fst>>fst)
            |> Seq.max
            |> (+) 1L
            
    let rec private shift offset ptr (vertices: Vertex<'a>[]) =
        if offset >= vertices.Length then offset // append at the end
        else
            let (next, _) = vertices.[offset]
            if next < ptr then offset
            else shift (offset+1) ptr vertices // move insertion point to the right
        
    let rec handle (replicaId: ReplicaId) (doc: Doc) (cmd: Command) =
        match cmd with
        | Assign value -> Assigned value
        | Remove -> Removed (doc |> List.map (fun e -> e.Timestamp))
        | Update(key, nested) ->
            let map = Doc.getObject doc |> Option.defaultValue Map.empty
            let inner = Map.tryFind key map |> Option.defaultValue []
            Updated(key, handle replicaId inner nested)
        | UpdateAt(idx, nested) ->
            let array = Doc.getArray doc |> Option.get // we cannot update index at array which doesn't exist
            let idx = indexWithTombstones idx array
            UpdatedAt(fst array.[idx], handle replicaId doc nested)
        | InsertAt(_, Remove) -> failwith "cannot insert and remove element at the same time"
        | InsertAt(idx, nested) ->
            let array = Doc.getArray doc |> Option.defaultValue [||] // we cannot update index at array which doesn't exist
            let idx = indexWithTombstones idx array
            let ptr = (nextSeqNr array, replicaId)
            let prev = if idx >= array.Length then None else Some (fst array.[idx-1])
            InsertedAt(prev, ptr, handle replicaId doc nested)
            
    let rec apply replicaId (timestamp: VTime) (doc: Doc) (op: Operation) =
        match op with
        | Assigned value -> Doc.assign timestamp value doc
        | Removed timestamps -> Doc.tombstone timestamps doc
        | Updated(key, nested) ->
            let map = doc |> Doc.getObject |> Option.defaultValue Map.empty
            let map' =
                match Map.tryFind key map with
                | None ->
                    let inner = apply replicaId timestamp [] nested
                    Map.add key inner map
                | Some e ->
                    let inner = apply replicaId timestamp e nested
                    Map.add key inner map
            let concurrent =
                (timestamp, doc)
                ||> Doc.set (function
                        | Array a  ->
                            let b = a |> Array.map (fun (vptr, doc) -> (vptr, Doc.tombstone [timestamp] doc))
                            Some (Array b)
                        | Object _ -> None)
            { Timestamp = timestamp; Value = Some (Object map') }::concurrent
            
        | UpdatedAt(ptr, nested) ->
            let array = Doc.getArray doc |> Option.defaultValue [||]
            let idx = indexOfVPtr ptr array
            let item = apply replicaId timestamp (snd array.[idx]) nested
            let array' = Array.replace idx (ptr, item) array
            let concurrent =
                (timestamp, doc)
                ||> Doc.set (function
                        | Array a -> None
                        | Object a ->
                            let b = a |> Map.map (fun k doc -> Doc.tombstone [timestamp] doc)
                            Some (Object b))
            { Timestamp = timestamp; Value = Some (Array array') }::concurrent
            
        | InsertedAt(prev, ptr, nested) ->
            let array = Doc.getArray doc |> Option.defaultValue [||]
            // find index where predecessor vertex can be found
            let predecessorIdx =
                match prev with
                | Some p -> indexOfVPtr p array
                | None   -> 0
            // adjust index where new vertex is to be inserted
            let insertIdx =
                let i = if predecessorIdx = 0 then 0 else predecessorIdx+1
                shift i ptr array
            // update RGA to store the highest observed sequence number
            let item = apply replicaId timestamp [] nested
            let array' = Array.insert insertIdx (ptr, item) array
            let concurrent = 
                (timestamp, doc)
                ||> Doc.set (function
                        | Array a -> None
                        | Object a ->
                            let b = a |> Map.map (fun k doc -> Doc.tombstone [timestamp] doc)
                            Some (Object b))
            { Timestamp = timestamp; Value = Some (Array array') }::concurrent
    
    let private crdt (replicaId: ReplicaId) : Crdt<Doc, Json, Command, Operation> =
        { new Crdt<_,_,_,_> with
            member _.Default = Doc.empty      
            member _.Query doc = valueOf doc |> Option.defaultValue (Json.Value Primitive.Null) 
            member _.Prepare(doc, cmd) = handle replicaId doc cmd
            member _.Effect(doc, e) = apply replicaId e.Version doc e.Data
        }
        
    type Endpoint = Endpoint<Doc, Command, Operation>
    
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    /// Inserts an `item` at given index. To insert at head use 0 index,
    /// to push back to a tail of sequence insert at array length. 
    let request (ref: Endpoint) (cmd: Command) : Async<Json> = ref <? Command cmd
    
    /// Retrieve an array of elements maintained by the given `ref` endpoint. 
    let query (ref: Endpoint) : Async<Json> = ref <? Query