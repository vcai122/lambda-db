{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Monad (forever)
import Data.List (intercalate)
import Database (Database (..), Row (..), createTable, emptyDatabase, executeQuery, insertRow)
import Query (Query (..), TableResult (..), Value (..), VariableName (..), parseQuery, prettyPrint)
import System.IO (hFlush, stdout)

main :: IO ()
main = do
  putStrLn "Welcome to the lambda-db Demo!"

  -- Create initial empty db
  let db0 = emptyDatabase

  -- Create 'classes' table
  let db1 =
        Database.createTable
          (VariableName "classes")
          (VariableName "id")
          [VariableName "id", VariableName "classname"]
          db0

  -- Insert some classes
  let classesRows =
        [ Row (IntVal 101) [VariableName "id", VariableName "classname"] [IntVal 101, StringVal "Math"],
          Row (IntVal 102) [VariableName "id", VariableName "classname"] [IntVal 102, StringVal "English"],
          Row (IntVal 103) [VariableName "id", VariableName "classname"] [IntVal 103, StringVal "History"]
        ]
  let db2 = foldr (insertRow (VariableName "classes")) db1 classesRows

  -- students table
  let db3 =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
          db2

  -- Insert some students
  let studentsRows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 1, StringVal "Arman", IntVal 85, IntVal 101],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 2, StringVal "Rohan", IntVal 90, IntVal 103],
          Row
            (IntVal 3)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 3, StringVal "Vincent", IntVal 95, IntVal 101],
          Row
            (IntVal 4)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 4, StringVal "Stephanie", IntVal 100, IntVal 102],
          Row
            (IntVal 5)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 5, StringVal "Swap", IntVal 98, IntVal 102],
          Row
            (IntVal 6)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 6, StringVal "Steve", IntVal 99, IntVal 103],
          Row
            (IntVal 7)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "class_id"]
            [IntVal 7, StringVal "Gary", IntVal 93, IntVal 101]
        ]
  let db4 = foldr (insertRow (VariableName "students")) db3 studentsRows

  -- Teacher table
  let db5 =
        Database.createTable
          (VariableName "teachers")
          (VariableName "id")
          [VariableName "id", VariableName "t_name", VariableName "subject"]
          db4

  -- Insert some teachers
  let teachersRows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "t_name", VariableName "subject"]
            [IntVal 1, StringVal "Stephanie Weirich", StringVal "CIS"],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "t_name", VariableName "subject"]
            [IntVal 2, StringVal "Robert Ghrist", StringVal "Math"],
          Row
            (IntVal 3)
            [VariableName "id", VariableName "t_name", VariableName "subject"]
            [IntVal 3, StringVal "Andreas Haeberlen", StringVal "NETS"]
        ]
  let db = foldr (insertRow (VariableName "teachers")) db5 teachersRows

  putStrLn "Database initialized with 'students', 'classes', and 'teachers' tables."
  putStrLn "You can now enter SQL queries. Type 'exit' or 'quit' to terminate."

  loop db

loop :: Database -> IO ()
loop db = do
  putStr "sql> "
  hFlush stdout
  line <- getLine
  if line == "exit" || line == "quit"
    then do
      putStrLn "Goodbye!"
      return ()
    else do
      let q = parseQuery line
      case q of
        SELECT tr -> do
          let (dbAfter, rows) = executeQuery q db
          printRows rows
          loop dbAfter
        CREATE {} -> do
          let (dbAfter, _) = executeQuery q db
          putStrLn "Table created."
          loop dbAfter
        INSERT {} -> do
          let (dbAfter, _) = executeQuery q db
          putStrLn "Rows inserted."
          loop dbAfter

-- A helper function to print rows in a table format
printRows :: [Row] -> IO ()
printRows [] = putStrLn "No rows returned."
printRows rs@(Row _ cols _ : _) = do
  putStrLn $ intercalate " | " (map (\(VariableName c) -> c) cols)
  putStrLn $ replicate (length cols * 10) '-'
  mapM_ printRow rs
  where
    printRow (Row _ cs vs) = do
      let vals = map showVal vs
      putStrLn (intercalate " | " vals)
    showVal (IntVal i) = show i
    showVal (DoubleVal d) = show d
    showVal (BoolVal b) = if b then "TRUE" else "FALSE"
    showVal (StringVal s) = s
    showVal NilVal = "NULL"
