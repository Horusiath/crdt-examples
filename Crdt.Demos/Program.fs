/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

open Crdt.Demos

[<EntryPoint>]
let main argv =
    CommutativeDoc.main() |> Async.RunSynchronously
    0 // return an integer exit code
