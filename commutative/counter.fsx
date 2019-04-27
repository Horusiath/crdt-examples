/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

type Counter = int64

[<RequireQualifiedAccess>]
module Counter =

  /// Operation to be transmitted and applied by all CRDT `Counter` replicas.
  type Op = Delta of int64

  /// Default instance of CmRDT counter.
  let empty: Counter = 0L

  /// Returns a value of CmRDT counter.
  let value (c: Counter) = c

  /// Returns and operation representing CRDT Counter increment operation.
  let inc (delta: uint32) = Delta (int64 delta)
  
  /// Returns and operation representing CRDT Counter decrement operation.
  let dec (delta: uint32) = Delta -(int64 delta)
  
  /// Applies submitted operation to a CRDT `Counter`.
  let downstream (c: Counter) (Delta d) = c + d