/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Commutative.Pure.RTree

open Akkling
open Crdt

type Path = string[]

type Operation =
    | Move of src:Path option * dst:Path option
    
type Node =
    { name: string
      children: Map<string, Node> }
    
[<RequireQualifiedAccess>]
module Node =
    let empty = { name = ""; children = Map.empty }
    
    let rec private takeLoop (path: Path) (current: Node) (i: int) (removed: Node option byref) =
        if i >= path.Length then current
        else
            let name = path.[i]
            match Map.tryFind name current.children with
            | None -> current
            | Some child ->
                let child = (takeLoop path child (i+1) &removed)
                { current with children = Map.add name child current.children }
    
    /// Removes a node under specified `path` from a given `root` node.
    /// Returns a tuple of updated `root` node and a removed node if it existed.
    let take (path: Path) (root: Node) : (Node * Node option) =
        let mutable removed = None
        let root = takeLoop path root 0 &removed
        (root, removed)
        
    let put (path: Path) children (root: Node) =
        let rec loop (path: Path) children (current: Node) i =
            if i = path.Length - 1 then
                { name = path.[i]; children = children }
            else
                let name = path.[i]
                let child =
                    match Map.tryFind name current.children with
                    | None -> { name = name; children = Map.empty }
                    | Some child -> child
                let child = loop path children child (i+1)
                { current with children = Map.add name child current.children }
        loop path children root 0
    
/// Check if `a` is ancestor of `b`.
let private isAncestor (a: Path) (b: Path) =
    let rec loop (a: Path) (b: Path) (i: int) =
        if i < a.Length && i < b.Length then
            if a.[i] = b.[i] then loop a b (i+1)
            else false
        elif i < a.Length then false
        else true
    loop a b 0

let private apply (node: Node) (op: Event<Operation>) =
    match op.Value with
    | Move(None, Some path) -> Node.put path Map.empty node // create new empty node
    | Move(Some path, None) -> fst (Node.take path node) // delete the node
    // move node in the tree structure (potentially changing its name)
    | Move(Some src, Some dst) ->
        let (node, target) = Node.take src node
        let children =
            target
            |> Option.map(fun n -> n.children)
            |> Option.defaultValue Map.empty
        Node.put dst children node
    
let crdt =
    { new PureCrdt<Node, Operation> with 
        member this.Default = Node.empty
        member this.Apply(state, ops) = ops |> Set.fold apply state
        member this.Obsoletes(o, n) =
            match n.Value, o.Value with
            // if two events target the same source node, use Last Write Wins strategy
            | Move(Some src1, _), Move(Some src2, _) when src1 = src2 -> n < o
            // if two events target the same destination node, use Last Write Wins strategy
            | Move(_, Some dst1), Move(_, Some dst2) when dst1 = dst2 -> n < o
            // check for cycles - if two move operations could result in a cycle, First Write Wins
            | Move(Some src1, Some dst1), Move(Some src2, Some dst2) when isAncestor src1 src2 && isAncestor dst2 dst1 -> o < n
            | _ -> false
    }
    
type Endpoint = Endpoint<Node, Operation>
    
/// Used to create replication endpoint handling operation-based ORSet protocol.
let props replicaId ctx = Replicator.actor crdt replicaId ctx

let query (ref: Endpoint) : Async<Node> = ref <? Query

/// Creates a new empty node at the given `path`.
let create (path: string) (ref: Endpoint) : Async<Node> =
    ref <? Submit (Move(None, Some (path.Split '/')))

/// Removes a node at the given `path`.    
let delete (path: string) (ref: Endpoint) : Async<Node> = ref <? Submit (Move(Some (path.Split '/'), None))

/// Move node from `src` path into `dst` path - if last path segment changes, the source changes its name.
let move (src: string) (dst: string) (ref: Endpoint) : Async<Node> = ref <? Submit (Move(Some (src.Split '/'), Some (dst.Split '/')))

let node name children =
    { name = name; children = children |> List.map (fun c -> c.name, c) |> Map.ofList }