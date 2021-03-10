/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt.Commutative

open System
open System.Text
open Crdt
open Akkling

/// Primitive type that can be assigned to a register.
[<RequireQualifiedAccess>]
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
    | String s -> "'" + s + "'"
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
          sb.Append(key).Append(": ") |> ignore
          stringify sb value
        while e.MoveNext() do
          sb.Append(", ") |> ignore
          let (key, value) = e.Current
          sb.Append(key).Append(": ") |> ignore
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
      
module Doc =
          
  /// Virtual pointer used to uniquely identify array elements. 
  [<Struct;CustomComparison;CustomEquality>]
  type VPtr =
    { Sequence: byte[]; Id: ReplicaId }
    override this.ToString() =
      String.Join('.', this.Sequence) + ":" + string this.Id 
    member this.CompareTo(other) =
      let len = min this.Sequence.Length other.Sequence.Length
      let mutable i = 0
      let mutable cmp = 0
      while cmp = 0 && i < len do
        cmp <- this.Sequence.[i].CompareTo other.Sequence.[i]
        i <- i + 1
      if cmp = 0 then
        // one of the sequences is subsequence of another one, compare their 
        // lengths (cause maybe they're the same) then compare replica ids
        cmp <- this.Sequence.Length - other.Sequence.Length
        if cmp = 0 then this.Id.CompareTo other.Id else cmp
      else cmp
    interface IComparable<VPtr> with member this.CompareTo other = this.CompareTo other
    interface IComparable with member this.CompareTo other = match other with :? VPtr as vptr -> this.CompareTo(vptr)
    interface IEquatable<VPtr> with member this.Equals other = this.CompareTo other = 0
      
  type Vertex<'a> = (VPtr * 'a)
      
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
  
  type Entry =
    | Leaf of VTime * Primitive
    | Array of VTime list * Vertex<Node>[]
    | Object of VTime list * Map<string,Node>
  
  /// Node represents a single JSON document graph element, be it a primitive value, array item or
  /// map key-value. Node represents a multi-value register semantics, meaning that in case of
  /// concurrent updates all of them are stored until conflict is resolved.
  /// This comes with a special treatment for containers (arrays and maps), which are not duplicated,
  /// instead their update/remove operations are propagated downwards to leaf nodes. Containers
  /// maintain add-wins semantics.
  and Node = Entry list
  
  [<RequireQualifiedAccess>]
  module Entry =
    
    /// Check if entry happened before given timestamp.
    let isBefore (time: VTime) (e: Entry) =
      match e with
      | Leaf(ts, _) -> Version.compare ts time < Ord.Eq
      | Array(ts, _)
      | Object(ts, _) -> ts |> List.forall (fun ts -> Version.compare ts time < Ord.Eq)
      
  [<RequireQualifiedAccess>]
  module Node =
    
    /// A new empty node.
    let empty : Node = []
      
    /// Assigns a primitive value to a current node, using multi-value register semantics. This means
    /// that all values that are in causal past of provided `timestamp` will be removed (if they are
    /// primitive values) or tombstoned (if they are container types). Tombstoning is necessary to
    /// resolve concurrent conflicting nested updates. 
    let assign (timestamp: VTime) (value: Primitive) (node: Node) : Node =
      let concurrent = node |> List.filter (fun e -> not (Entry.isBefore timestamp e))
      Leaf(timestamp, value)::concurrent
      
    let getObject (node: Node) =
      node
      |> List.tryFind (function Object _ -> true | _ -> false)
      |> Option.map (fun (Object(_, map)) -> map)
    
    let getArray (node: Node) =
      node
      |> List.tryFind (function Array _ -> true | _ -> false)
      |> Option.map (fun (Array(_, vertices)) -> vertices)
      
    let timestamps (node: Node) =
      node
      |> List.collect (function
        | Leaf(ts, _)   -> [ts]
        | Array(ts, _)  -> ts
        | Object(ts, _) -> ts)
      
    /// Recursively tombstones current node and all of its contents (if it has container kinds like
    /// array or map) using provided tombstone timestamps. Elements in causal future to provided
    /// timestamps are not tombstoned, as well as the concurrent ones, meaning this operation
    /// maintains add wins semantics.
    let rec removeNode (tombstone: VTime) (node: Node) : Node option =
      let node = node |> List.choose (fun e ->
        match e with
        | Leaf(timestamp, _) ->
          // remove node with timestamp lower than tombstone
          if Version.compare timestamp tombstone <= Ord.Eq then None else Some e
        | Array(timestamps, vertices) ->
          // check if all of array's timestamps are behind tombstone
          if timestamps |> List.forall (fun ts -> Version.compare ts tombstone < Ord.Eq) then
            None // remove node with lower timestamp
          else
            // recursivelly check if other array elements need removal
            let vertices =
              vertices
              |> Array.choose (fun (ptr, node) -> removeNode tombstone node |> Option.map (fun n -> (ptr, n)))
            Some(Array(timestamps, vertices))
        | Object(timestamps, fields) -> 
          // check if all of object's timestamps are behind tombstone
          if timestamps |> List.forall (fun ts -> Version.compare ts tombstone < Ord.Eq) then
            None // remove node with lower timestamp
          else
            // recursivelly check if other object fields needs removal
            let fields = fields |> Map.fold (fun acc key value ->
              match removeNode tombstone value with
              | None -> acc
              | Some v -> Map.add key v acc) Map.empty 
            Some(Object(timestamps, fields))
      )
      // if all node entries were removed remove node itself
      if List.isEmpty node then None else Some node

  type Command =
    | Assign   of Primitive              // assign primitive value
    | Remove                             // remove current node, doesn't work when combined with `InsertAt`
    | Update   of key:string * Command   // insert or update entry with a given key in the map component of a node
    | UpdateAt of index:int * Command    // update existing item at a given index in the array component of a node
    | InsertAt of index:int * Command    // insert a new item at a given index in the array component of a node
    
  type Operation =
    | AtKey     of string * Operation
    | AtIndex   of VPtr * Operation
    | Assigned  of Primitive
    | Removed
    
  /// Materialized a CRDT into a user-friednly JSON-like data type.
  let rec valueOf (node: Node) : Json option =
    let out =
      node
      |> List.choose (fun e ->
        match e with
        | Leaf(_, v) -> Some (Json.Value v)
        | Array(_, a)  ->
          let b = a |> Array.choose (fun (_, node) -> valueOf node )
          if Array.isEmpty b then None else Some (Json.Array b)
        | Object(_, a) ->
          let b =
            a
            |> Seq.choose (fun e -> valueOf e.Value |> Option.map (fun v -> (e.Key, v)))
            |> Map.ofSeq
          if Map.isEmpty b then None else Some (Json.Obj b))

    match out with
    | []  -> None
    | [x] -> Some (x)
    | conflicts -> Some (Json.Concurrent conflicts)
    
  let rec handle (replicaId: ReplicaId) (node: Node) (cmd: Command) =
    match cmd with
    | Assign value -> Assigned value
    | Remove -> Removed
    | Update(key, nested) ->
      // get Object component of the node or create it
      let map = Node.getObject node |> Option.defaultValue Map.empty
      let inner = Map.tryFind key map |> Option.defaultValue []
      AtKey(key, handle replicaId inner nested)
    | UpdateAt(i, nested) ->
      let array = Node.getArray node |> Option.get // we cannot update index at array which doesn't exist
      let (ptr, inner) = array.[i]
      AtIndex(ptr, handle replicaId inner nested)
    | InsertAt(_, Remove) -> failwith "cannot insert and remove element at the same time"
    | InsertAt(i, nested) ->
      // get Array component of the node or create it
      let array = Node.getArray node |> Option.defaultValue [||]
      // LSeq generates VPtr based on the VPtrs of the preceding and following indexes
      let left = if i = 0 then [||] else (fst array.[i-1]).Sequence  
      let right = if i = array.Length then [||] else (fst array.[i]).Sequence
      let ptr = { Sequence = generateSeq left right; Id = replicaId }
      AtIndex(ptr, handle replicaId Node.empty nested)
            
  let rec apply replicaId (timestamp: VTime) (node: Node) (op: Operation) : Node =
    match op with
    | Assigned value -> Node.assign timestamp value node
    
    | Removed ->  
      Node.removeNode timestamp node |> Option.defaultValue Node.empty
      
    | AtKey(key, nested) ->
      // get Object component from the node or create it
      let timestamps, map =
        match List.tryFind (function Object _ -> true | _ -> false) node with
        | Some(Object(timestamps, map)) -> (timestamps, map)
        | _ -> ([], Map.empty)
      let innerNode = Map.tryFind key map |> Option.defaultValue []
      // apply inner event update and update current object
      let map = Map.add key (apply replicaId timestamp innerNode nested) map
      // override non-concurrent timestamps of a current object component
      let timestamps = timestamp::(List.filter (fun ts -> Version.compare ts timestamp = Ord.Cc) timestamps)
      // override non-concurrent entries of a current node
      let concurrent =
        node |> List.choose (fun e ->
          match e with
          | Object _ -> None // we cover object update separately
          | outdated when Entry.isBefore timestamp outdated -> None
          | other -> Some other)
      // attach modified object entry to result node
      (Object(timestamps, map))::concurrent
      
    | AtIndex(ptr, nested) ->
      // get Array component from the node or create it
      let mutable timestamps, array =
        match List.tryFind (function Array _ -> true | _ -> false) node with
        | Some(Array(timestamps, array)) -> (timestamps, array)
        | _ -> ([], [||])
      // find index of a given VPtr or index where it should be inserted
      let i = array |> Array.binarySearch (fun (x, _) -> ptr >= x)
      if i < Array.length array && fst array.[i] = ptr then
        // we're updating existing node
        array <- Array.copy array // defensive copy for future update
        let (_, entry) = array.[i]
        array.[i] <- (ptr, apply replicaId timestamp entry nested)
      else
        // we're inserting new node
        let n = (ptr, apply replicaId timestamp Node.empty nested)
        array <- Array.insert i n array
      // override non-concurrent entries of a current node
      let concurrent =
        node |> List.choose (fun e ->
          match e with
          | Array _ -> None // we cover array entry update separately
          | outdated when Entry.isBefore timestamp outdated -> None
          | other -> Some other)
      // attach modified object entry to result node
      (Array(timestamps, array))::concurrent
  
  let private crdt (replicaId: ReplicaId) : Crdt<Node, Json, Command, Operation> =
    { new Crdt<_,_,_,_> with
      member _.Default = Node.empty    
      member _.Query node = valueOf node |> Option.defaultValue (Json.Value Primitive.Null) 
      member _.Prepare(node, cmd) = handle replicaId node cmd
      member _.Effect(node, e) = apply replicaId e.Version node e.Data
    }
    
  type Endpoint = Endpoint<Node, Command, Operation>
  
  /// Used to create replication endpoint handling operation-based JSON protocol.
  let props db replicaId ctx = replicator (crdt replicaId) db replicaId ctx
   
  /// Inserts an `item` at given index. To insert at head use 0 index,
  /// to push back to a tail of sequence insert at array length. 
  let request (cmd: Command) (ref: Endpoint) : Async<Json> = ref <? Command cmd
  
  /// Retrieve an array of elements maintained by the given `ref` endpoint. 
  let query (ref: Endpoint) : Async<Json> = ref <? Query