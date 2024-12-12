# LambdaDB

_A simple database implemented in Haskell supporting a subset of SQL._

**Authors**: Vincent Cai (caiv), Arman Ashaboglu (armanash), Rohan Moniz (rmoniz) 

## Overview
- `src/BTree.hs`: contains the BTree implementation, which is the underlying data structure for the database
- `src/Query.hs`: contains the framework for parsing, representing, and pretty-printing SQL queries
- `src/Database.hs`: contains the main database logic for creating and manipulating tables, inserting and querying rows, and other SQL queries

  
  - The entry point for the executable is in [Main.hs](app/Main.hs). 
  
  - All of your test cases are in [the test directory](test/Spec.hs).

## Building, running, and testing

This project compiles with `stack build`. 
You can run the main executable with `stack run`.
You can run the tests with `stack test`. 

Finally, you can start a REPL with `stack ghci`.

