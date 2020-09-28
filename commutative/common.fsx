/// The MIT License (MIT)
/// Copyright (c) 2018 Bartosz Sypytkowski

namespace Crdt

#load "../common.fsx"

/// A contaienr for user-defined events of type 'e. It contains metadata necessary
/// to partially order and distribute events in peer-to-peer fashion. 
type Event<'e> =
    { /// The replica, on which this event originally was created.
      Origin: ReplicaId
      /// The sequence number given by the origin replica at the moment of event creation.
      /// This allows us to keep track of replication progress with remote replicas even
      /// when we didn't received their events directly, but via intermediate replica. 
      OriginSeqNr: uint64
      /// The sequence number given by the local replica. For events created by current replica
      /// it's the same as `OriginSeqNr`. For replicated events it's usually higher.
      LocalSeqNr: uint64
      /// Vector clock which describes happened-before relationships between events from
      /// different replicas, enabling to establish partial order among them.
      Version: VTime
      /// An user-defined event data.
      Data: 'e }
    override this.ToString() = sprintf "(%s, %i, %i, %A, %O)" this.Origin this.OriginSeqNr this.LocalSeqNr this.Version this.Data 

[<Interface>]
type Crdt<'crdt,'state,'cmd,'event> =
    /// Get a default (zero) value of the CRDT.
    abstract Default: 'crdt
    /// Given a CRDT state return an actual value that user has interest in. Eg. ORSet still has to carry
    /// metadata timestamps, however from user perspective materialized value of ORSet is just ordrinary Set<'a>. 
    abstract Query: 'crdt -> 'state
    /// Equivalent of command handler in eventsourcing analogy.
    abstract Prepare: state:'crdt * command:'cmd -> 'event
    /// Equivalent of event handler in eventsourcing analogy.
    abstract Effect: state:'crdt * event:Event<'event> -> 'crdt