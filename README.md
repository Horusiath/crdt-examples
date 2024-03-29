# CRDT Examples

This repository contains an example implementation of various CRDTs. It serves
mainly academic purposes (the implementations are meant to be simple and easy to
understand, not optimized). If you want help or want to actually use them on
production please let me know ;)

List of CRDTs (implemented and to be implemented):

1. Convergent (state-based):

   - Delta-based
     - [x] Grow-only Counter
     - [x] Positive/Negative Counter
     - [x] Grow-only Set
     - [x] Add Wins Observed Remove Set
     - [x] Multi Value Register
     - [x] YATA
   - [x] Grow-only Counter
   - [x] Positive/Negative Counter
   - [x] Bounded Counter
   - [x] Grow-only Set
   - [x] 2 Phase Set
   - [x] Add Wins Observed Removed Set
   - [x] Last Write Wins Register

1. Commutative (operation-based)
   - Pure-operation based:
     - [ ] protocol: Tagged Reliable Causal Broadcast
     - [ ] Counter
     - [ ] Observed Remove Set
   - [x] protocol: Reliable Causal Broadcast
   - [x] Counter
   - [x] Last Write Wins Register
   - [x] Multi Value Register
   - [x] Observed Remove Set (add wins semantics)
   - [x] Linear Sequence (L-Seq)
   - [x] Replicated Growable Array (RGA)
   - [x] Replicated Growable Array (blockwise variant)
   - [x] JSON-like document

## How to run

### Requirements

- Docker

### Running

1. Start Docker
2. From inside the repo, run the following command

```sh
$ docker build -t crdt5 .
```

3. Run the following command to start the container

```sh
$ docker run -it crdt5
```

4. Inside the container, run the following command to run the tests

```sh
$ dotnet Crdt.Tests.dll --debug --sequenced
```
