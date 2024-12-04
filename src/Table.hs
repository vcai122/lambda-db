module Table where

import BTree (BTree, deleteBTree, insertBTree, lookupBTree)
import Block (Block, BlockID, deserializeBlock, serializeBlock)
import qualified Data.Map as M
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck

-- | Table representation using BTree's and indexes etc.
data Table = Table
  { tableName :: String,
    primaryIndex :: BTree Int BlockID, -- Primary index
    secondaryIndices :: M.Map String (BTree Int BlockID) --- Secondary indices
  }
  deriving (Eq, Show)

-- | Error in table operations
data TableError
  = ColumnNotIndexed String
  | KeyNotFound Int
  | InvalidOperation String
  deriving (Eq, Show)

-- | Parse a file to table
parseFileToTable :: String -> IO Table
parseFileToTable = undefined

-- | Serialize table into file
serializeTableToFile :: Table -> IO String
serializeTableToFile = undefined

-- | Insert key-value into table
insertIntoTable :: Table -> Int -> [(String, String)] -> IO (Either TableError Table)
insertIntoTable table key values = undefined

-- | Lookup value in table by key
lookupInTable :: Table -> Int -> IO (Either TableError [(String, String)])
lookupInTable table key = undefined

-- | Delete key-value from table
deleteFromTable :: Table -> Int -> IO (Either TableError Table)
deleteFromTable table key = undefined

-- | Add secondary index to column
addIndex :: Table -> String -> IO (Either TableError Table)
addIndex table columnName = undefined

--- Testing:

-- Unit tests for Table.hs
test_insertIntoTable :: Test
test_insertIntoTable =
  "Insert into table"
    ~: let table = Table "ExampleTable" emptyBTree M.empty
           key = 1
           values = [("name", "Arman"), ("age", "21")]
        in case insertIntoTable table key values of
             Left err -> assertFailure $ "Unexpected error: " ++ show err
             Right updatedTable -> True

test_lookupInTable :: Test
test_lookupInTable =
  "Lookup in table"
    ~: let table = Table "ExampleTable" emptyBTree M.empty
           key = 1
        in case lookupInTable table key of
             Left err -> err ~?= KeyNotFound key
             Right _ -> assertFailure "Should not find a value for key 1 in empty table"

tests :: Test
tests = TestList [test_insertIntoTable, test_lookupInTable]

main :: IO Counts
main = runTestTT tests

-- QuickCheck properties:

-- Inserting key-value does not delete existing rows
prop_insertPreservesRows :: Table -> Int -> [(String, String)] -> IO Bool
prop_insertPreservesRows table key values = do
  let originalKeys = map fst $ toList (primaryIndex table)
  updatedTable <- insertIntoTable table key values
  case updatedTable of
    Left _ -> pure True
    Right table' -> do
      let updatedKeys = map fst $ toList (primaryIndex table')
      pure $ all (`elem` updatedKeys) originalKeys

-- Lookup gives the inserted value
prop_insertLookup :: Table -> Int -> [(String, String)] -> IO Bool
prop_insertLookup table key values = do
  updatedTable <- insertIntoTable table key values
  case updatedTable of
    Left _ -> pure False
    Right table' -> do
      result <- lookupInTable table' key
      pure $ result == Right values

-- Adding index should NOT modify existing data
prop_addIndexPreservesData :: Table -> String -> IO Bool
prop_addIndexPreservesData table columnName = do
  originalData <- traverse (`lookupInTable` table) (keys $ primaryIndex table)
  updatedTable <- addIndex table columnName
  case updatedTable of
    Left _ -> pure True
    Right table' -> do
      updatedData <- traverse (`lookupInTable` table') (keys $ primaryIndex table')
      pure $ originalData == updatedData

-- Parsing file into table and then serializing the table back to file should be same file
prop_fileToTableToFile :: String -> IO Bool
prop_fileToTableToFile fileContent = do
  table <- parseFileToTable fileContent
  serializedFile <- serializeTableToFile table
  pure $ fileContent == serializedFile

-- Serializing table to file then parsing it back into table should be same table
prop_tableToFileToTable :: Table -> IO Bool
prop_tableToFileToTable table = do
  serializedFile <- serializeTableToFile table
  parsedTable <- parseFileToTable serializedFile
  pure $ equivalentTables table parsedTable
