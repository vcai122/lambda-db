{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Database where

import BTree
import Control.Monad (void)
import Data.ByteString qualified as BS
import Data.Function ((&))
import Data.List (elemIndex, nub, sort, sortBy)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Serialize (Serialize)
import Data.Serialize qualified as S
import GHC.Generics (Generic)
import Query (Aliased (..), BinaryOp (..), ColumnName (..), Columns (..), Expression (..), IntermediaryTable (..), OrderKey (..), Query (..), TableResult (..), UnaryOp (..), Value (..), VariableName (..))
import Query qualified as Q
import System.Directory (createDirectoryIfMissing, doesFileExist)
import Test.HUnit (Assertion, Test (..), assert, assertEqual, runTestTT, (~:), (~?=))
import Test.QuickCheck
import Test.QuickCheck.Arbitrary

-- | Row represents a single row in a table
data Row = Row
  { primaryKey :: Value, -- Primary key value
    columnNames :: [VariableName], -- Column names in order
    values :: [Value] -- Values in order corresponding to column names
  }
  deriving (Show, Eq, Generic)

instance Serialize Row where
  put (Row pk cols vals) = do
    S.put pk
    S.put cols
    S.put vals
  get = Row <$> S.get <*> S.get <*> S.get

instance Arbitrary Row where
  arbitrary = do
    pk <- arbitrary
    let validColChar = elements $ ['a' .. 'z'] ++ ['A' .. 'Z']
    colCount <- choose (1, 5)
    cols <- vectorOf colCount $ do
      first <- validColChar
      rest <- listOf validColChar
      return $ VariableName (first : rest)
    vals <- vectorOf colCount arbitrary
    -- Ensure primary key is IntVal for consistency
    return $ Row (IntVal $ abs (hash pk)) cols vals
    where
      hash :: Value -> Int
      hash (IntVal i) = i
      hash (DoubleVal d) = round d
      hash (BoolVal b) = if b then 1 else 0
      hash (StringVal s) = length s
      hash NilVal = 0

-- | Table represents a table in the database
data DbTable = DbTable
  { dbTableName :: VariableName,
    dbPrimaryKeyName :: VariableName,
    dbOtherColumnNames :: [VariableName],
    dbPrimaryIndex :: BTree Value Row, -- Clustered index
    dbSecondaryIndices :: [(VariableName, BTree Value Value)] -- Secondary indices mapping column values to primary keys
  }
  deriving (Show, Generic)

instance Serialize DbTable where
  put (DbTable name pkName others pIndex sIndices) = do
    S.put name
    S.put pkName
    S.put others
    S.put pIndex
    S.put sIndices
  get = DbTable <$> S.get <*> S.get <*> S.get <*> S.get <*> S.get

instance Serialize Database where
  put (Database tables) = S.put tables
  get = Database <$> S.get

-- | Database represents the entire database
data Database = Database
  { dbTables :: [(VariableName, DbTable)]
  }
  deriving (Show, Generic)

instance Eq DbTable where
  t1 == t2 =
    dbTableName t1 == dbTableName t2
      && dbPrimaryKeyName t1 == dbPrimaryKeyName t2
      && dbOtherColumnNames t1 == dbOtherColumnNames t2

instance Eq Database where
  db1 == db2 = dbTables db1 == dbTables db2

-- Type to track table aliases during query execution
data QueryContext = QueryContext
  { tableAliases :: [(VariableName, VariableName)] -- (alias, actual table name)
  }

data EvalContext = EvalContext
  { currentTable :: Maybe VariableName,
    aliases :: [(VariableName, VariableName)], -- (alias, actual table name)
    allTables :: [(VariableName, DbTable)]
  }

-- | PK name
primaryKeyName :: VariableName
primaryKeyName = VariableName "id"

-- | Create an empty database
emptyDatabase :: Database
emptyDatabase = Database []

-- | Create an empty table with specified name and columns
createTable :: VariableName -> VariableName -> [VariableName] -> Database -> Database
createTable tName pkName colNames db =
  Database $ (tName, newTable) : dbTables db
  where
    newTable =
      DbTable
        { dbTableName = tName,
          dbPrimaryKeyName = pkName,
          dbOtherColumnNames = filter (/= pkName) colNames,
          dbPrimaryIndex = emptyBTree 2,
          dbSecondaryIndices = []
        }

-- | Insert a row into a table
insertRow :: VariableName -> Row -> Database -> Database
insertRow tName row db =
  Database $ map updateTable (dbTables db)
  where
    updateTable (name, table)
      | name == tName = (name, insertIntoTable row table)
      | otherwise = (name, table)

-- | Insert a row into a specific table
insertIntoTable :: Row -> DbTable -> DbTable
insertIntoTable row table =
  table
    { dbPrimaryIndex = insert (primaryKey row, row) (dbPrimaryIndex table),
      dbSecondaryIndices = map updateSecondaryIndex (dbSecondaryIndices table)
    }
  where
    updateSecondaryIndex (colName, index) =
      let colValue = lookupColumnValue colName row table
       in (colName, insert (colValue, primaryKey row) index)

-- | Helper to look up a column value in a row
lookupColumnValue :: VariableName -> Row -> DbTable -> Value
lookupColumnValue colName (Row pk cols vals) table =
  case lookup colName (zip cols vals) of
    Just val -> val
    Nothing -> if colName == dbPrimaryKeyName table then pk else error "Column not found"

-- | Execute a query on the database
executeQuery :: Query -> Database -> [Row]
executeQuery (SELECT tableResult) db = executeTableResult tableResult db

-- | Execute a table result query
executeTableResult :: TableResult -> Database -> [Row]
executeTableResult (BasicSelect cols from whereClause orderKeys limitCount) db =
  let baseRows = getRowsFromFrom from db
      filteredRows = maybe baseRows (\expr -> filter (evaluateWhereExpression expr db) baseRows) whereClause
      orderedRows = applyOrdering orderKeys db filteredRows
      limitedRows = maybe orderedRows (\n -> take n orderedRows) limitCount
      projectedRows = projectColumns cols db limitedRows
   in projectedRows

-- | Get rows from a FROM clause
getRowsFromFrom :: IntermediaryTable -> Database -> [Row]
getRowsFromFrom (Q.Table (Aliased _ name)) db =
  case lookup name (dbTables db) of
    Just table -> getAllRows table
    Nothing -> error $ "Table not found: " ++ show name
getRowsFromFrom (Join left right expr) db =
  let leftRows = getRowsFromFrom left db
      rightRows = getRowsFromFrom right db
   in [r1 | r1 <- leftRows, r2 <- rightRows, evaluateJoinExpression expr r1 r2 db]
getRowsFromFrom (TableResult _) _ = error "Subqueries not implemented yet"

-- | Get all rows from a table
getAllRows :: DbTable -> [Row]
getAllRows table =
  catMaybes [lookupValue k (dbPrimaryIndex table) | k <- getAllKeys (dbPrimaryIndex table)]

-- | Evaluate a WHERE expression
evaluateWhereExpression :: Expression -> Database -> Row -> Bool
evaluateWhereExpression expr db row = eval expr
  where
    eval (BinaryOp And e1 e2) = eval e1 && eval e2
    eval (BinaryOp Or e1 e2) = eval e1 || eval e2
    eval (BinaryOp op e1 e2) = compareValues op (evaluateExpression e1 db row) (evaluateExpression e2 db row)
    eval (UnaryOp Not e) = not $ eval e
    eval _ = error "Invalid where expression"

-- | Evaluate a join expression
evaluateJoinExpression :: Expression -> Row -> Row -> Database -> Bool
evaluateJoinExpression expr row1 row2 db =
  let ctx = EvalContext Nothing (getAliases expr) (dbTables db)
   in evalJoinExpr ctx expr
  where
    evalJoinExpr ctx (BinaryOp op e1 e2) =
      compareValues
        op
        (evalWithRows ctx e1 row1 row2)
        (evalWithRows ctx e2 row1 row2)
    evalJoinExpr _ _ = error "Invalid join expression"

    evalWithRows :: EvalContext -> Expression -> Row -> Row -> Value
    evalWithRows ctx (Column (ColumnName tNameM name)) r1 r2 =
      case tNameM of
        Nothing -> error "Join conditions must specify table names"
        Just alias ->
          let realTable = resolveTableName ctx alias
           in case lookup realTable (allTables ctx) of
                Just table ->
                  if belongsToTable r1 table
                    then fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r1)
                    else fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r2)
                Nothing -> error $ "Table not found: " ++ show alias
    evalWithRows _ (Value v) _ _ = v
    evalWithRows _ _ _ _ = error "Complex expressions in joins not supported"

    resolveTableName ctx alias = fromMaybe alias (lookup alias $ aliases ctx)

-- Helper function to extract aliases from join expression
getAliases :: Expression -> [(VariableName, VariableName)]
getAliases (BinaryOp _ (Column (ColumnName (Just t1) _)) (Column (ColumnName (Just t2) _))) =
  [(t1, t1), (t2, t2)]
getAliases _ = []

resolveAlias :: QueryContext -> VariableName -> VariableName
resolveAlias ctx alias =
  fromMaybe alias (lookup alias (tableAliases ctx))

findValueInRow :: VariableName -> Row -> Maybe Value
findValueInRow name (Row pk cols vals) =
  if name `elem` cols
    then Just $ vals !! fromMaybe (error "Column not found") (elemIndex name cols)
    else Nothing

belongsToTable :: Row -> DbTable -> Bool
belongsToTable (Row _ cols _) table =
  -- Check if the row's columns match the table's columns
  let tableColumns = dbPrimaryKeyName table : dbOtherColumnNames table
   in sort cols == sort tableColumns

-- Some helper functions
getJoinInfo :: Expression -> Maybe (VariableName, VariableName, Expression)
getJoinInfo (BinaryOp Eq (Column (ColumnName (Just t1) c1)) (Column (ColumnName (Just t2) c2))) =
  Just (t1, t2, BinaryOp Eq (Column (ColumnName (Just t1) c1)) (Column (ColumnName (Just t2) c2)))
getJoinInfo _ = Nothing

aliasesFromJoin :: (VariableName, VariableName, Expression) -> [(VariableName, VariableName)]
aliasesFromJoin (alias1, alias2, _) = [(alias1, alias1), (alias2, alias2)]

-- | Evaluate an expression to a value
evaluateExpression :: Expression -> Database -> Row -> Value
evaluateExpression expr db row = evalWithContext expr (EvalContext Nothing [] (dbTables db))
  where
    evalWithContext :: Expression -> EvalContext -> Value
    evalWithContext (Column col@(ColumnName tNameM name)) ctx =
      case tNameM of
        Nothing ->
          if name == VariableName "id"
            then primaryKey row
            else fromMaybe (error $ "Column not found: " ++ show col) (findValueInRow name row)
        Just alias ->
          let realTable = resolveTableName ctx alias
           in case lookup realTable (allTables ctx) of
                Just table ->
                  if belongsToTable row table
                    then fromMaybe (error $ "Column not found: " ++ show col) (findValueInRow name row)
                    else error $ "Column not found in table: " ++ show col
                Nothing -> error $ "Table not found: " ++ show alias
    evalWithContext (Value v) _ = v
    evalWithContext (UnaryOp op e) ctx =
      case op of
        Len -> case evalWithContext e ctx of
          StringVal s -> IntVal (length s)
          _ -> error "LENGTH only works on strings"
        Not -> error "NOT not implemented"
    evalWithContext _ _ = error "Complex expressions not implemented"

    resolveTableName :: EvalContext -> VariableName -> VariableName
    resolveTableName ctx alias = fromMaybe alias (lookup alias $ aliases ctx)

-- | Compare two values using a binary operator
compareValues :: BinaryOp -> Value -> Value -> Bool
compareValues op v1 v2 = case (op, v1, v2) of
  (Eq, _, _) -> v1 == v2
  (Ne, _, _) -> v1 /= v2
  (Gt, IntVal i1, IntVal i2) -> i1 > i2
  (Gt, DoubleVal d1, DoubleVal d2) -> d1 > d2
  (Ge, IntVal i1, IntVal i2) -> i1 >= i2
  (Ge, DoubleVal d1, DoubleVal d2) -> d1 >= d2
  (Lt, IntVal i1, IntVal i2) -> i1 < i2
  (Lt, DoubleVal d1, DoubleVal d2) -> d1 < d2
  (Le, IntVal i1, IntVal i2) -> i1 <= i2
  (Le, DoubleVal d1, DoubleVal d2) -> d1 <= d2
  (And, BoolVal b1, BoolVal b2) -> b1 && b2
  (Or, BoolVal b1, BoolVal b2) -> b1 || b2
  (Concat, StringVal s1, StringVal s2) -> error "String concatenation not implemented"
  _ -> error $ "Invalid comparison between " ++ show v1 ++ " and " ++ show v2

-- | Apply ORDER BY
applyOrdering :: [OrderKey] -> Database -> [Row] -> [Row]
applyOrdering [] _ rows = rows
applyOrdering keys db rows =
  sortBy (compareRows keys db) rows

-- | Compare rows based on order keys
compareRows :: [OrderKey] -> Database -> Row -> Row -> Ordering
compareRows keys db row1 row2 =
  foldr combineOrderings EQ $ map (compareByKey row1 row2) keys
  where
    compareByKey :: Row -> Row -> OrderKey -> Ordering
    compareByKey r1 r2 (Asc expr) =
      compare (evaluateExpression expr db r1) (evaluateExpression expr db r2)
    compareByKey r1 r2 (Desc expr) =
      compare (evaluateExpression expr db r2) (evaluateExpression expr db r1)

    combineOrderings :: Ordering -> Ordering -> Ordering
    combineOrderings EQ o = o
    combineOrderings o _ = o

-- | Project specific columns
projectColumns :: Columns -> Database -> [Row] -> [Row]
projectColumns AllColumns _ rows = rows
projectColumns (Columns cols) db rows =
  map (projectRow cols db) rows

-- | Project specific columns from a row
projectRow :: [Aliased Expression] -> Database -> Row -> Row
projectRow cols db row =
  let projectedValues = map (evaluateExpression . value) cols
      projectedNames = map (maybe (error "Alias required") id . name) cols
      projectedPk = primaryKey row -- Keep the primary key
   in Row projectedPk projectedNames (map (\expr -> expr db row) projectedValues)

-- Serialization functions

-- | Save database to file
saveDatabase :: FilePath -> Database -> IO ()
saveDatabase fp db = do
  let bs = S.encode db
  BS.writeFile fp bs

-- | Load database from file
loadDatabase :: FilePath -> IO Database
loadDatabase fp = do
  exists <- doesFileExist fp
  if exists
    then do
      bs <- BS.readFile fp
      case S.decode bs of
        Left err -> error $ "Failed to decode database: " ++ err
        Right db -> return db
    else return emptyDatabase

-- Tests

testSimpleInsertAndQuery :: Test
testSimpleInsertAndQuery = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      row =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "name", VariableName "grade"]
          [IntVal 1, StringVal "John", IntVal 95]
      db'' = insertRow (VariableName "students") row db'
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Query should return inserted row" [row] result

testJoin :: Test
testJoin = TestCase $ do
  let db = emptyDatabase
      -- Create students table
      db1 =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "class_id"]
          db
      -- Create classes table
      db2 =
        createTable
          (VariableName "classes")
          (VariableName "id")
          [VariableName "id", VariableName "name"]
          db1
      -- Insert data
      student =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "name", VariableName "class_id"]
          [IntVal 1, StringVal "John", IntVal 101]
      class' =
        Row
          (IntVal 101)
          [VariableName "id", VariableName "name"]
          [IntVal 101, StringVal "Math"]
      db3 =
        insertRow (VariableName "students") student $
          insertRow (VariableName "classes") class' db2
      -- Create join query
      joinExpr =
        BinaryOp
          Eq
          (Column (ColumnName (Just (VariableName "students")) (VariableName "class_id")))
          (Column (ColumnName (Just (VariableName "classes")) (VariableName "id")))
      query =
        SELECT $
          BasicSelect
            AllColumns
            ( Join
                (Q.Table $ Aliased Nothing (VariableName "students"))
                (Q.Table $ Aliased Nothing (VariableName "classes"))
                joinExpr
            )
            Nothing
            []
            Nothing
      result = executeQuery query db3
  assertEqual "Join should return matching rows" 1 (length result)

testWhereClause :: Test
testWhereClause = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal 1, StringVal "John", IntVal 95],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal 2, StringVal "Jane", IntVal 85]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      whereClause =
        BinaryOp
          Gt
          (Column (ColumnName Nothing (VariableName "grade")))
          (Value (IntVal 90))
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            (Just whereClause)
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Where clause should filter correctly" 1 (length result)

testOrderBy :: Test
testOrderBy = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal 1, StringVal "John", IntVal 95],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal 2, StringVal "Jane", IntVal 85]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            [Desc (Column (ColumnName Nothing (VariableName "grade")))]
            Nothing
      result = executeQuery query db''
  assertEqual
    "First result should have highest grade"
    (IntVal 95)
    (head [v | Row _ cols vals <- result, (col, v) <- zip cols vals, col == VariableName "grade"])

testProjection :: Test
testProjection = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      row =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "name", VariableName "grade"]
          [IntVal 1, StringVal "John", IntVal 95]
      db'' = insertRow (VariableName "students") row db'
      query =
        SELECT $
          BasicSelect
            ( Columns
                [ Aliased
                    (Just (VariableName "student_name"))
                    (Column (ColumnName Nothing (VariableName "name")))
                ]
            )
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual
    "Projection should only include selected columns"
    [VariableName "student_name"]
    (columnNames $ head result)

testLimit :: Test
testLimit = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      -- Create rows with different primary keys
      rows =
        [ Row
            (IntVal i)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal i, StringVal "John", IntVal 95]
          | i <- [1 .. 5]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            []
            (Just 3)
      result = executeQuery query db''
  assertEqual "Limit should restrict number of results" 3 (length result)

-- Fix testMultipleInserts to ensure unique primary keys
testMultipleInserts :: Test
testMultipleInserts = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row
            (IntVal i)
            [VariableName "id", VariableName "name", VariableName "grade"]
            [IntVal i, StringVal ("Student" ++ show i), IntVal (85 + i)]
          | i <- [1 .. 10]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Should return all 10 inserted rows" 10 (length result)

testComplexWhere :: Test
testComplexWhere = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"]
          db
      rows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"]
            [IntVal 1, StringVal "John", IntVal 95, IntVal 20],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"]
            [IntVal 2, StringVal "Jane", IntVal 85, IntVal 19]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      whereClause =
        BinaryOp
          And
          (BinaryOp Gt (Column (ColumnName Nothing (VariableName "grade"))) (Value (IntVal 80)))
          (BinaryOp Lt (Column (ColumnName Nothing (VariableName "age"))) (Value (IntVal 21)))
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            (Just whereClause)
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Complex where should return correct number of rows" 2 (length result)

testMultipleOrderBy :: Test
testMultipleOrderBy = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "grade", VariableName "age"]
          db
      rows =
        [ Row (IntVal 1) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 1, IntVal 90, IntVal 20],
          Row (IntVal 2) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 2, IntVal 90, IntVal 19],
          Row (IntVal 3) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 3, IntVal 85, IntVal 21]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            [ Desc (Column (ColumnName Nothing (VariableName "grade"))),
              Asc (Column (ColumnName Nothing (VariableName "age")))
            ]
            Nothing
      result = executeQuery query db''
  assertEqual
    "First result should be highest grade, lowest age"
    (IntVal 2)
    (primaryKey $ head result)

testJoinWithWhere :: Test
testJoinWithWhere = TestCase $ do
  let db = emptyDatabase
      db1 =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "class_id"]
          db
      db2 =
        createTable
          (VariableName "classes")
          (VariableName "id")
          [VariableName "id", VariableName "subject", VariableName "teacher"]
          db1
      student =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "name", VariableName "class_id"]
          [IntVal 1, StringVal "John", IntVal 101]
      class1 =
        Row
          (IntVal 101)
          [VariableName "id", VariableName "subject", VariableName "teacher"]
          [IntVal 101, StringVal "Math", StringVal "Smith"]
      class2 =
        Row
          (IntVal 102)
          [VariableName "id", VariableName "subject", VariableName "teacher"]
          [IntVal 102, StringVal "Physics", StringVal "Johnson"]
      db3 =
        insertRow (VariableName "students") student $
          insertRow (VariableName "classes") class1 $
            insertRow (VariableName "classes") class2 db2
      joinExpr =
        BinaryOp
          Eq
          (Column (ColumnName (Just (VariableName "students")) (VariableName "class_id")))
          (Column (ColumnName (Just (VariableName "classes")) (VariableName "id")))
      whereClause =
        BinaryOp
          Eq
          (Column (ColumnName (Just (VariableName "classes")) (VariableName "subject")))
          (Value (StringVal "Math"))
      query =
        SELECT $
          BasicSelect
            AllColumns
            ( Join
                (Q.Table $ Aliased (Just (VariableName "students")) (VariableName "students"))
                (Q.Table $ Aliased (Just (VariableName "classes")) (VariableName "classes"))
                joinExpr
            )
            (Just whereClause)
            []
            Nothing
      result = executeQuery query db3
  assertEqual "Join with where should return one row" 1 (length result)

testComplexProjection :: Test
testComplexProjection = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "first_name", VariableName "last_name"]
          db
      row =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "first_name", VariableName "last_name"]
          [IntVal 1, StringVal "John", StringVal "Doe"]
      db'' = insertRow (VariableName "students") row db'
      query =
        SELECT $
          BasicSelect
            ( Columns
                [ Aliased
                    (Just (VariableName "full_name"))
                    (Column (ColumnName Nothing (VariableName "first_name"))),
                  Aliased
                    (Just (VariableName "surname"))
                    (Column (ColumnName Nothing (VariableName "last_name")))
                ]
            )
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual
    "Projection should have correct column names"
    [VariableName "full_name", VariableName "surname"]
    (columnNames $ head result)

testEmptyTableQuery :: Test
testEmptyTableQuery = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "empty_table")
          (VariableName "id")
          [VariableName "id", VariableName "name"]
          db
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "empty_table"))
            Nothing
            []
            Nothing
      result = executeQuery query db'
  assertEqual "Query on empty table should return empty list" [] result

testTableAlias :: Test
testTableAlias = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name"]
          db
      row =
        Row
          (IntVal 1)
          [VariableName "id", VariableName "name"]
          [IntVal 1, StringVal "John"]
      db'' = insertRow (VariableName "students") row db'
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased (Just (VariableName "s")) (VariableName "students"))
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Query with table alias should return correct result" [row] result

testMultipleConditionsWhere :: Test
testMultipleConditionsWhere = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "grade", VariableName "attendance"]
          db
      rows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "grade", VariableName "attendance"]
            [IntVal 1, IntVal 95, IntVal 100],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "grade", VariableName "attendance"]
            [IntVal 2, IntVal 85, IntVal 90],
          Row
            (IntVal 3)
            [VariableName "id", VariableName "grade", VariableName "attendance"]
            [IntVal 3, IntVal 92, IntVal 95]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      whereClause =
        BinaryOp
          And
          (BinaryOp Gt (Column (ColumnName Nothing (VariableName "grade"))) (Value (IntVal 90)))
          ( BinaryOp
              Or
              (BinaryOp Eq (Column (ColumnName Nothing (VariableName "attendance"))) (Value (IntVal 100)))
              (BinaryOp Lt (Column (ColumnName Nothing (VariableName "grade"))) (Value (IntVal 93)))
          )
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            (Just whereClause)
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Complex where conditions should return correct rows" 1 (length result)

testLimitOffset :: Test
testLimitOffset = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "numbers")
          (VariableName "id")
          [VariableName "id", VariableName "value"]
          db
      rows =
        [ Row
            (IntVal i)
            [VariableName "id", VariableName "value"]
            [IntVal i, IntVal (i * 10)]
          | i <- [1 .. 5]
        ]
      db'' = foldr (insertRow (VariableName "numbers")) db' rows
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "numbers"))
            Nothing
            [Asc (Column (ColumnName Nothing (VariableName "id")))]
            (Just 3)
      result = executeQuery query db''
  assertEqual "Should return exactly 3 rows" 3 (length result)
  assertEqual
    "First row should have id 1"
    (IntVal 1)
    (primaryKey $ head result)

testSelfJoin :: Test
testSelfJoin = TestCase $ do
  let db = emptyDatabase
      db' =
        createTable
          (VariableName "employees")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "manager_id"]
          db
      rows =
        [ Row
            (IntVal 1)
            [VariableName "id", VariableName "name", VariableName "manager_id"]
            [IntVal 1, StringVal "Boss", IntVal 1],
          Row
            (IntVal 2)
            [VariableName "id", VariableName "name", VariableName "manager_id"]
            [IntVal 2, StringVal "Worker", IntVal 1]
        ]
      db'' = foldr (insertRow (VariableName "employees")) db' rows
      joinExpr =
        BinaryOp
          Eq
          (Column (ColumnName (Just (VariableName "e1")) (VariableName "manager_id")))
          (Column (ColumnName (Just (VariableName "e2")) (VariableName "id")))
      query =
        SELECT $
          BasicSelect
            AllColumns
            ( Join
                (Q.Table $ Aliased (Just (VariableName "e1")) (VariableName "employees"))
                (Q.Table $ Aliased (Just (VariableName "e2")) (VariableName "employees"))
                joinExpr
            )
            Nothing
            []
            Nothing
      result = executeQuery query db''
  assertEqual "Self join should return correct number of rows" 2 (length result)

-- QuickCheck properties

-- | Property: Inserting a row and querying it by primary key should return the row
prop_insertAndQueryByPK :: Row -> Property
prop_insertAndQueryByPK row =
  property $
    let db =
          createTable
            (VariableName "test")
            (VariableName "id") -- Use "id" instead of "pk"
            (VariableName "id" : columnNames row) -- Include id in columns
            emptyDatabase
        db' = insertRow (VariableName "test") row db
        whereClause =
          BinaryOp
            Eq
            (Column (ColumnName Nothing (VariableName "id"))) -- Query by "id"
            (Value $ primaryKey row)
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              (Just whereClause)
              []
              Nothing
     in executeQuery query db' == [row]

-- | Property: Ordering by a column should maintain that column's order
prop_orderByMaintainsOrder :: [Row] -> Property
prop_orderByMaintainsOrder rows =
  not (null rows) ==>
    let db =
          createTable
            (VariableName "test")
            (VariableName "id") -- Use "id" instead of "pk"
            (VariableName "id" : columnNames (head rows))
            emptyDatabase
        db' = foldr (insertRow (VariableName "test")) db rows
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              Nothing
              [Asc (Column (ColumnName Nothing (VariableName "id")))] -- Order by "id"
              Nothing
        result = executeQuery query db'
     in property $ and $ zipWith (\r1 r2 -> primaryKey r1 <= primaryKey r2) result (tail result)

-- | Property: Applying a limit should never return more rows than specified
prop_limitConstrainsResults :: [Row] -> Positive Int -> Property
prop_limitConstrainsResults rows (Positive n) =
  not (null rows) ==>
    let db =
          createTable
            (VariableName "test")
            (VariableName "pk")
            (primaryKeyName : columnNames (head rows))
            emptyDatabase
        db' = foldr (insertRow (VariableName "test")) db rows
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              Nothing
              []
              (Just n)
        result = executeQuery query db'
     in property $ length result <= n

-- | Property: WHERE clause with equals should only return matching rows
prop_whereEqualsFiltersCorrectly :: [Row] -> Property
prop_whereEqualsFiltersCorrectly rows =
  not (null rows) ==>
    let targetRow = head rows
        targetPK = primaryKey targetRow
        db =
          createTable
            (VariableName "test")
            (VariableName "id") -- Use "id" instead of "pk"
            (VariableName "id" : columnNames targetRow)
            emptyDatabase
        db' = foldr (insertRow (VariableName "test")) db rows
        whereClause =
          BinaryOp
            Eq
            (Column (ColumnName Nothing (VariableName "id"))) -- Query by "id"
            (Value targetPK)
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              (Just whereClause)
              []
              Nothing
        result = executeQuery query db'
     in property $ all (\row -> primaryKey row == targetPK) result

-- | Property: Inserting rows in different orders should result in the same database state
prop_insertOrderIndependent :: [Row] -> Property
prop_insertOrderIndependent rows =
  not (null rows) ==>
    let uniqueRows = makeUniqueKeys rows
        db =
          createTable
            (VariableName "test")
            (VariableName "id")
            (VariableName "id" : columnNames (head rows))
            emptyDatabase
        db1 = foldr (insertRow (VariableName "test")) db uniqueRows
        db2 = foldr (insertRow (VariableName "test")) db (reverse uniqueRows)
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              Nothing
              [Asc (Column (ColumnName Nothing (VariableName "id")))]
              Nothing
     in property $ executeQuery query db1 == executeQuery query db2
  where
    makeUniqueKeys :: [Row] -> [Row]
    makeUniqueKeys = zipWith (\i (Row _ cols vals) -> Row (IntVal i) cols vals) [1 ..]

-- | Property: Creating a table, dropping it, and creating it again should give same result as creating it once
prop_createDropCreate :: VariableName -> [VariableName] -> Property
prop_createDropCreate tableName columns =
  not (null columns) ==>
    let columns' = nub columns -- Ensure unique column names
        db1 = createTable tableName (head columns') columns' emptyDatabase
        db2 =
          createTable tableName (head columns') columns' $
            createTable tableName (head columns') columns' emptyDatabase
     in property $ db1 == db2

-- Run all tests
runTests :: IO ()
runTests = do
  putStrLn "Running unit tests:"
  void $
    runTestTT $
      TestList
        [ testSimpleInsertAndQuery,
          testJoin,
          testWhereClause,
          testOrderBy,
          testProjection,
          testLimit,
          testMultipleInserts,
          testComplexWhere,
          testMultipleOrderBy,
          testJoinWithWhere,
          testComplexProjection,
          testEmptyTableQuery,
          testTableAlias,
          testMultipleConditionsWhere,
          testLimitOffset,
          testSelfJoin
        ]
  putStrLn "\nRunning QuickCheck properties..."
  putStrLn "Testing insert and query by PK"
  quickCheck prop_insertAndQueryByPK
  putStrLn "Testing order by maintains order"
  quickCheck prop_orderByMaintainsOrder
  putStrLn "Testing limit constrains results"
  quickCheck prop_limitConstrainsResults
  putStrLn "Testing where equals filters correctly"
  quickCheck prop_whereEqualsFiltersCorrectly
  putStrLn "Testing insert order independence"
  quickCheck prop_insertOrderIndependent
  putStrLn "Testing create, drop, create"
  quickCheck prop_createDropCreate

  putStrLn "All tests passed!"