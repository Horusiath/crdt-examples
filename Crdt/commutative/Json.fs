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
        | Concurrent of Json list // a special case for handling concurrent value updates
        | Array of Json[]
        | Obj of Map<string, Json>
        /// Create JSON-like string out of this one. The only major difference is concurrent case,
        /// in which concurrent values will be formatted as (<value1> | <value2> | etc.) 
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
    
    /// Virtual pointer used to uniquely identify array elements. 
    type VPtr = (int64 * ReplicaId)
    
    /// Just like in case of RGA, vertices are used to mark corresponding insertions with their unique IDs. 
    type Vertex<'a> = (VPtr * 'a)
        
    /// Entry is used to mark an operation element with a timestamp. It's also used to keep tombstones
    /// around - in that case `Value` is None. 
    [<Struct>]   
    type Entry<'a> =
        { Value: 'a option
          Timestamp: VTime }
        
    /// Entry kind is used to determine what kind of update do we have there. Since JSON doesn't have
    /// a static structure, it's possible that the same field will be updated with different data types
    /// on concurrent operations eg. `x.field = 0` on one node and `x.field = {}` on another one.
    and EntryKind =
        | Values of Primitive
        | Array of Vertex<Node>[]
        | Object of Map<string, Node>
     
    /// Node represents a single JSON document graph element, be it a primitive value, array item or
    /// map key-value. Node represents a multi-value register semantics, meaning that in case of
    /// concurrent updates all of them are stored until conflict is resolved.
    /// This comes with a special treatment for containers (arrays and maps), which are not duplicated,
    /// instead their update/remove operations are propagated downwards to leaf nodes. Containers
    /// maintain add-wins semantics.
    and Node = Entry<EntryKind> list
        
    [<RequireQualifiedAccess>]
    module Entry =
        
        let inline create timestamp value = { Value = Some value; Timestamp = timestamp }
                    
        let inline update (timestamp: VTime) value (entry: Entry<_>) =
            { Value = Some value; Timestamp = Version.merge timestamp entry.Timestamp }
        
    [<RequireQualifiedAccess>]
    module Node =
        
        /// A new empty node.
        let empty : Node = []
        
        /// Returns all values stored in current node. Unless a conflicting update of primive values has
        /// occurred, this should return a list with 1 element.
        let getValues (node: Node) =
            node |> List.choose (fun e -> match e.Value with Some(Values p) -> Some p | _ -> None)
            
        /// Returns a map object stored under a given node, unless this node doesn't represent map type.
        let getObject (node: Node) =
            node
            |> List.choose (fun e -> match e.Value with Some(Object o) -> Some o | _ -> None)
            |> List.tryHead
            
        /// Returns an array stored under a given node, unless this node doesn't represent array type.
        let getArray (node: Node) =
            node
            |> List.choose (fun e -> match e.Value with Some(Array o) -> Some o | _ -> None)
            |> List.tryHead
                    
        /// Assigns a primitive value to a current node, using multi-value register semantics. This means
        /// that all values that are in causal past of provided `timestamp` will be removed (if they are
        /// primitive values) or tombstoned (if they are container types). Tombstoning is necessary to
        /// resolve concurrent conflicting nested updates. 
        let assign (timestamp: VTime) (value: Primitive) (node: Node) : Node =
            let concurrent =
                node
                |> List.choose (fun e ->
                    match e.Value with
                    | Some(Values x) when Version.compare e.Timestamp timestamp <= Ord.Eq -> None
                    | Some(Values x) -> Some e
                    | Some other when Version.compare e.Timestamp timestamp <= Ord.Eq -> Some { e with Value = None }
                    | _ -> Some e
                )
            { Value = Some(Values value); Timestamp = timestamp }::concurrent
                        
        /// Recursively tombstones current node and all of its contents (if it has container kinds like
        /// array or map) using provided tombstone timestamps. Elements in causal future to provided
        /// timestamps are not tombstoned, as well as the concurrent ones, meaning this operation
        /// maintains add wins semantics.
        let rec tombstone (timestamps: VTime list) (node: Node) : Node =
            node
            |> List.choose (fun e ->
                match timestamps |> List.tryFind (fun ts -> Version.compare e.Timestamp ts <= Ord.Eq) with
                | Some timestamp ->
                    match e.Value with
                    | Some (Values _) -> None
                    | Some (Array a)  ->
                        let b = a |> Array.map (fun (vptr, node) -> (vptr, tombstone timestamps node))
                        Some { Timestamp = timestamp; Value = Some (Array b) }
                    | Some (Object a) -> 
                        let b = a |> Map.map (fun key node -> tombstone timestamps node)
                        Some { Timestamp = timestamp; Value = Some (Object b) }
                    | None -> Some { e with Timestamp = timestamp }
                | None ->
                    // not found, try deeper
                    match e.Value with
                    | Some (Array a)  ->
                        let b = a |> Array.map (fun (vptr, node) -> (vptr, tombstone timestamps node))
                        Some { e with Value = Some (Array b) }
                    | Some (Object a) -> 
                        let b = a |> Map.map (fun key node -> tombstone timestamps node)
                        Some { e with Value = Some (Object b) }
                    | other -> Some e)
           
        /// Helper function used to modify an entry.
        let set (modify: EntryKind -> EntryKind option) (timestamp: VTime) (node: Node) : Node =
            node
            |> List.choose (fun e ->
                match e.Value with
                | None -> Some { e with Timestamp = Version.max e.Timestamp timestamp }
                | Some (Values _) when Version.compare e.Timestamp timestamp <= Ord.Eq -> None
                | Some other -> modify other |> Option.map (fun o -> { e with Timestamp = Version.max e.Timestamp timestamp; Value = Some o }))
        
        /// Checks if current node is fully tombstoned.   
        let isTombstoned (node: Node) = node |> List.forall (fun e -> Option.isNone e.Value)

    type Command =
        | Assign   of Primitive                // assign primitive value
        | Update   of key:string * Command     // insert or update entry with a given key in the map component of a node
        | UpdateAt of index:int * Command      // update existing item at a given index in the array component of a node
        | InsertAt of index:int * Command      // insert a new item at a given index in the array component of a node
        | Remove                               // remove current node, doesn't work when combined with `InsertAt`
        
    type Operation =
        | Updated   of key:string * Operation
        | InsertedAt of predecessor:VPtr option * current:VPtr * Operation
        | UpdatedAt of VPtr * Operation
        | Assigned  of Primitive
        | Removed   of VTime list
        
    /// Materialized a CRDT into a user-friednly JSON-like data type.
    let rec valueOf (node: Node) : Json option =
        let out =
            node
            |> List.choose (fun e ->
                match e.Value with
                | None -> None
                | Some (Values v) -> Some (Json.Value v)
                | Some (Array a)  ->
                    let b = a |> Array.choose (fun (_, node) -> valueOf node)
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
        let rec loop offset remaining (vertices: Vertex<Node>[]) =
            if remaining = 0 then offset
            elif Node.isTombstoned (snd vertices.[offset]) then loop (offset+1) remaining vertices // skip over tombstones
            else loop (offset+1) (remaining-1) vertices
        loop 0 index vertices // skip head as it's always tombstoned (it serves as reference point)
    
    /// Maps user-given VIndex into physical index inside of `vertices` array.
    let private indexOfVPtr ptr vertices =
        let rec loop offset ptr (vertices: Vertex<'a>[]) =
            if ptr = fst vertices.[offset] then offset
            else loop (offset+1) ptr vertices
        loop 0 ptr vertices
       
    /// Returns a new sequence number for an insert to a given RGA. 
    let nextSeqNr (vertices: Vertex<'a>[]) =
        if Array.isEmpty vertices then 1L
        else
            vertices
            |> Array.toSeq
            |> Seq.map (fst>>fst)
            |> Seq.max
            |> (+) 1L
          
    /// RGA conditional index shifting - performed when two values where inserted concurrently
    /// at the same position. In that case we use VPtr of each inserted vertex to deterministally
    /// define their total position inside of RGA. 
    let rec private shift offset ptr (vertices: Vertex<'a>[]) =
        if offset >= vertices.Length then offset // append at the end
        else
            let (next, _) = vertices.[offset]
            if next < ptr then offset
            else shift (offset+1) ptr vertices // move insertion point to the right
        
    let rec handle (replicaId: ReplicaId) (node: Node) (cmd: Command) =
        match cmd with
        | Assign value -> Assigned value
        | Remove -> Removed (node |> List.map (fun e -> e.Timestamp))
        | Update(key, nested) ->
            let map = Node.getObject node |> Option.defaultValue Map.empty
            let inner = Map.tryFind key map |> Option.defaultValue []
            Updated(key, handle replicaId inner nested)
        | UpdateAt(idx, nested) ->
            let array = Node.getArray node |> Option.get // we cannot update index at array which doesn't exist
            let idx = indexWithTombstones idx array
            UpdatedAt(fst array.[idx], handle replicaId node nested)
        | InsertAt(_, Remove) -> failwith "cannot insert and remove element at the same time"
        | InsertAt(idx, nested) ->
            let array = Node.getArray node |> Option.defaultValue [||] // we cannot update index at array which doesn't exist
            let idx = indexWithTombstones idx array
            let ptr = (nextSeqNr array, replicaId)
            let prev = if idx >= array.Length then None else Some (fst array.[idx-1])
            InsertedAt(prev, ptr, handle replicaId node nested)
            
    let rec apply replicaId (timestamp: VTime) (node: Node) (op: Operation) =
        match op with
        | Assigned value -> Node.assign timestamp value node
        | Removed timestamps -> Node.tombstone timestamps node
        | Updated(key, nested) ->
            let map = node |> Node.getObject |> Option.defaultValue Map.empty
            let map' =
                match Map.tryFind key map with
                | None ->
                    let inner = apply replicaId timestamp [] nested
                    Map.add key inner map
                | Some e ->
                    let inner = apply replicaId timestamp e nested
                    Map.add key inner map
            let concurrent =
                (timestamp, node)
                ||> Node.set (function
                        | Array a  ->
                            let b = a |> Array.map (fun (vptr, node) -> (vptr, Node.tombstone [timestamp] node))
                            Some (Array b)
                        | Object _ -> None)
            { Timestamp = timestamp; Value = Some (Object map') }::concurrent
            
        | UpdatedAt(ptr, nested) ->
            let array = Node.getArray node |> Option.defaultValue [||]
            let idx = indexOfVPtr ptr array
            let item = apply replicaId timestamp (snd array.[idx]) nested
            let array' = Array.replace idx (ptr, item) array
            let concurrent =
                (timestamp, node)
                ||> Node.set (function
                        | Array a -> None
                        | Object a ->
                            let b = a |> Map.map (fun k node -> Node.tombstone [timestamp] node)
                            Some (Object b))
            { Timestamp = timestamp; Value = Some (Array array') }::concurrent
            
        | InsertedAt(prev, ptr, nested) ->
            let array = Node.getArray node |> Option.defaultValue [||]
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
                (timestamp, node)
                ||> Node.set (function
                        | Array a -> None
                        | Object a ->
                            let b = a |> Map.map (fun k node -> Node.tombstone [timestamp] node)
                            Some (Object b))
            { Timestamp = timestamp; Value = Some (Array array') }::concurrent
    
    let private crdt (replicaId: ReplicaId) : Crdt<Node, Json, Command, Operation> =
        { new Crdt<_,_,_,_> with
            member _.Default = Node.empty      
            member _.Query node = valueOf node |> Option.defaultValue (Json.Value Primitive.Null) 
            member _.Prepare(node, cmd) = handle replicaId node cmd
            member _.Effect(node, e) = apply replicaId e.Version node e.Data
        }
        
    type Endpoint = Endpoint<Node, Command, Operation>
    
    /// Used to create replication endpoint handling operation-based RGA protocol.
    let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
     
    /// Inserts an `item` at given index. To insert at head use 0 index,
    /// to push back to a tail of sequence insert at array length. 
    let request (ref: Endpoint) (cmd: Command) : Async<Json> = ref <? Command cmd
    
    /// Retrieve an array of elements maintained by the given `ref` endpoint. 
    let query (ref: Endpoint) : Async<Json> = ref <? Query