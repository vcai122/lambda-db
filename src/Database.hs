module Database where

import BTree (BTree)
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))


-- | TableName -> BTree.
newtype Database = Database (M.Map TableName BTree)
  deriving (Eq, Show)

-- | Error handling for database operations.
data DatabaseError
  = TableNotFound TableName
  | KeyNotFound String
  | InvalidOperation String
  deriving (Eq, Show)

-- | Return type for database operations
type DatabaseResult a = Either DatabaseError a

-- | Insert key-value pair into the table
insertDatabase :: Database -> TableName -> Int -> String -> DatabaseResult Database
insertDatabase = undefined

-- | Lookup value by key in a table
lookupDatabase :: Database -> TableName -> Int -> DatabaseResult String
lookupDatabase = undefined

-- | Delete key-value pair from a table
deleteDatabase :: Database -> TableName -> Int -> DatabaseResult Database
deleteDatabase = undefined

-- | Create new table in database
createTable :: Database -> TableName -> DatabaseResult Database
createTable = undefined

-- | Execute query on database
executeQuery :: Database -> Query -> DatabaseResult Database
executeQuery = undefined


-- | Testing:
table1 :: TableName
table1 = TableName "table1"

emptyDB :: Database
emptyDB = Database M.empty

mockBTree :: BTree
mockBTree = error "Define or use mock implementation"

test_insert :: Test
test_insert =
  "Insert into table"
    ~: case insertDatabase emptyDB table1 1 "Alice" of
      Left err -> err ~?= TableNotFound table1
      Right _ -> assertFailure "Should not insert into a non-existent table."

test_lookup :: Test
test_lookup =
  "Lookup in table"
    ~: case lookupDatabase emptyDB table1 1 of
      Left err -> err ~?= TableNotFound table1
      Right _ -> assertFailure "Should not find a key in a non-existent table."

test_delete :: Test
test_delete =
  "Delete from table"
    ~: case deleteDatabase emptyDB table1 1 of
      Left err -> err ~?= TableNotFound table1
      Right _ -> assertFailure "Should not delete from a non-existent table."

test_create_table :: Test
test_create_table =
  "Create new table"
    ~: case createTable emptyDB table1 of
      Left err -> assertFailure "Should successfully create a new table."
      Right db ->
        case M.lookup table1 (let (Database d) = db in d) of
          Nothing -> assertFailure "Table should exist after creation."
          Just _ -> assert "Table created successfully."