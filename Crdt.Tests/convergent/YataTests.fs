/// The MIT License (MIT)
/// Copyright (c) 2018-2021 Bartosz Sypytkowski

module Crdt.Tests.Convergent.YataTests

open Expecto
open Crdt.Convergent

let private str (array: char[]) = System.String(array)

[<Tests>]
let tests = testList "A convergent YATA array" [
    test "should insert elements at the beginning" {
        let a =
            Yata.zero
            |> Yata.insert "A" 0 'a'
            |> Yata.insert "A" 0 'b'
            |> Yata.insert "A" 0 'c'
        let actual = a |> Yata.value |> str
        Expect.equal actual "cba" "elements inserted at position 0 should appear in order"        
    }
    
    test "should insert elements at the end" {
        let a =
            Yata.zero
            |> Yata.insert "A" 0 'a'
            |> Yata.insert "A" 1 'b'
            |> Yata.insert "A" 2 'c'
        let actual = a |> Yata.value |> str
        Expect.equal actual "abc" "elements inserted at the end should appear in order"        
    }

    test "should insert elements in mixed order" {
        let a =
            Yata.zero
            |> Yata.insert "A" 0 'a'
            |> Yata.insert "A" 1 'b'
            |> Yata.insert "A" 1 'c'
        let actual = a |> Yata.value |> str
        Expect.equal actual "acb" "elements inserted should appear in order regardless of insertion time"        
    }
    
    test "should insert/delete elements" {
        let a =
            Yata.zero
            |> Yata.insert "A" 0 'a'
            |> Yata.insert "A" 1 'b'
            |> Yata.insert "A" 1 'c'
            |> Yata.delete 2
            |> Yata.insert "A" 2 'd'
        let actual = a |> Yata.value |> str
        Expect.equal actual "acd" "deleted elements should not appear in result set"        
    }
    
    
    ftest "should allow to merge elements" {
        let a =
            Yata.zero
            |> Yata.insert "A" 0 'a'
            |> Yata.insert "A" 1 'b'
            |> Yata.insert "A" 2 'c'
                    
        let b =
            a
            |> Yata.insert "B" 1 'd'
            |> Yata.insert "B" 2 'e'
            
        let a =
            a
            |> Yata.insert "A" 1 'f'
            |> Yata.delete 0
            
        let c1 = Yata.merge a b
        let c2 = Yata.merge b a
            
        let actual = c1 |> Yata.value |> str
        Expect.equal actual "dfebc" "merged conflicts should be resolved"
        Expect.equal c1 c2 "merge should be commutative"
    }
]