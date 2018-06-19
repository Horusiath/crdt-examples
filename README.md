This repository contains an example implementation of various CRDTs. It serves mainly academic purposes (the implementations are meant to be simple and easy to understand, not optimized). If you want help or want to actually use them on production please let me know ;)

List of CRDTs (implemented and to be implemented):

1. Convergent (state-based):
    - Delta-based
        - [x] Grow-only Counter
        - [x] Increment/Decrement Counter
        - [x] Grow-only Set
        - [x] Add Wins Observed Remove Set
        - [x] Multi Value Register
        - [ ] Bounded Counter
    - [x] Grow-only Counter
    - [x] Increment/Decrement Counter
    - [x] Grow-only Set
    - [x] 2 Phase Set
    - [x] Add Wins Observed Removed Set
    - [x] Last Write Wins Register

1. Commutative (operation-based)
    - [ ] Counter
    - [ ] Observed Remove Set
    - [ ] Linear Sequence (L-Seq)
    - [ ] Replicated Growable Array (block-wise variant)