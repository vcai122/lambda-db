{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Database where

import BTree
import Control.Monad (mplus, void)
import Data.ByteString qualified as BS
import Data.Function ((&))
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.List (elemIndex, nub, sort, sortBy)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Serialize (Serialize)
import Data.Serialize qualified as S
import GHC.Generics (Generic)
import Query
  ( Aliased (..),
    BinaryOp (..),
    ColumnName (..),
    Columns (..),
    Expression (..),
    IntermediaryTable (..),
    OrderKey (..),
    Query (..),
    TableResult (..),
    UnaryOp (..),
    Value (..),
    VariableName (..),
  )
import Query qualified as Q
import System.Directory (createDirectoryIfMissing, doesFileExist)
import System.IO.Unsafe (unsafePerformIO)
import Test.HUnit (Assertion, Test (..), assert, assertEqual, runTestTT, (~:), (~?=))
import Test.QuickCheck
import Test.QuickCheck.Arbitrary

-- | Row represents a single row in a table
data Row = Row
  { primaryKey :: Value, -- PK value
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
    colCount <- choose (1, 2)
    let validColChar = elements (['a' .. 'z'] ++ ['A' .. 'Z'])
    cols <- vectorOf colCount $ do
      first <- validColChar
      rest <- listOf validColChar
      return $ VariableName (first : rest)
    vals <- vectorOf colCount arbitrary
    let pkVal = case pk of
          IntVal i -> IntVal i
          DoubleVal d -> IntVal (round d)
          BoolVal True -> IntVal 1
          BoolVal False -> IntVal 0
          StringVal s -> IntVal (length s)
          NilVal -> IntVal 0
        allCols = VariableName "id" : cols
        allVals = pkVal : vals
    return $ Row pkVal allCols allVals

-- Represents a table in the database
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

instance Eq DbTable where
  t1 == t2 =
    dbTableName t1 == dbTableName t2
      && dbPrimaryKeyName t1 == dbPrimaryKeyName t2
      && dbOtherColumnNames t1 == dbOtherColumnNames t2

-- Database represents the entire database
data Database = Database
  { dbTables :: [(VariableName, DbTable)]
  }
  deriving (Show, Generic)

instance Eq Database where
  db1 == db2 = dbTables db1 == dbTables db2

instance Serialize Database where
  put (Database tables) = S.put tables
  get = Database <$> S.get

data EvalContext = EvalContext
  { currentTable :: Maybe VariableName,
    aliases :: [(VariableName, VariableName)],
    allTables :: [(VariableName, DbTable)]
  }

{-# NOINLINE globalPkCounter #-}
globalPkCounter :: IORef Int
globalPkCounter = unsafePerformIO (newIORef 0)

-- A helper to get the next global primary key
nextGlobalId :: IO Int
nextGlobalId = atomicModifyIORef' globalPkCounter (\x -> (x + 1, x + 1))

-- | PK name
primaryKeyName :: VariableName
primaryKeyName = VariableName "id"

-- Create an empty database
emptyDatabase :: Database
emptyDatabase = Database []

-- Create an empty table with specified name and columns
createTable :: VariableName -> VariableName -> [VariableName] -> Database -> Database
createTable tName pkName colNames db =
  Database $ (tName, newTable) : dbTables db
  where
    newTable =
      DbTable
        { dbTableName = tName,
          dbPrimaryKeyName = pkName,
          dbOtherColumnNames = filter (/= pkName) colNames,
          dbPrimaryIndex = emptyBTreeValRow 2,
          dbSecondaryIndices = []
        }

-- Insert row into a table
insertRow :: VariableName -> Row -> Database -> Database
insertRow tName row db =
  Database $ map updateTable (dbTables db)
  where
    updateTable (name, table)
      | name == tName = (name, insertIntoTable row table)
      | otherwise = (name, table)

-- Insert row into a specific table
insertIntoTable :: Row -> DbTable -> DbTable
insertIntoTable row table =
  -- Check if a row with the same primary key already exists
  case searchBTree (dbPrimaryIndex table) (primaryKey row) of
    Just _ ->
      table
    Nothing ->
      table
        { dbPrimaryIndex = insertBTree (primaryKey row) row (dbPrimaryIndex table),
          dbSecondaryIndices = map updateSecondaryIndex (dbSecondaryIndices table)
        }
  where
    updateSecondaryIndex (colName, index) =
      let colValue = lookupColumnValue colName row table
       in (colName, insertBTree colValue (primaryKey row) index)

-- Helper: looks up a column value in a row
lookupColumnValue :: VariableName -> Row -> DbTable -> Value
lookupColumnValue colName (Row pk cols vals) table =
  case lookup colName (zip cols vals) of
    Just val -> val
    Nothing ->
      if colName == dbPrimaryKeyName table
        then pk
        else error "Column not found"

-- Build alias map from IntermediaryTable for query parsing
buildAliasMap :: IntermediaryTable -> [(VariableName, VariableName)]
buildAliasMap (Q.Table (Aliased alias t)) =
  case alias of
    Just a -> [(a, t)]
    Nothing -> [(t, t)]
buildAliasMap (TableResult (Aliased alias _)) =
  []
buildAliasMap (Join l r _) =
  buildAliasMap l ++ buildAliasMap r

emptyBTreeValRow :: Int -> BTree Value Row
emptyBTreeValRow t = BTree t (BTreeNode [] [])

-- Execute a query on the database
executeQuery :: Query -> Database -> (Database, [Row])
executeQuery (SELECT tableResult) db =
  let (rows) = executeTableResult tableResult db
   in (db, rows)
executeQuery (INSERT tbl insertCols tResult) db =
  let insertedRows = executeTableResult tResult db
      Just table = lookup tbl (dbTables db)
      pkName = dbPrimaryKeyName table

      -- Modified code begins here:
      finalRows =
        map
          ( \r ->
              let (pkVal, otherVals) = case elemIndex pkName (columnNames r) of
                    Just idx ->
                      -- PK is provided in r
                      let providedPk = values r !! idx
                       in (providedPk, removeIndex idx (values r))
                    Nothing ->
                      -- PK not provided in this row, generate a new one
                      let newPk = IntVal (unsafePerformIO nextGlobalId)
                       in (newPk, values r)

                  newCols = pkName : (filter (/= pkName) insertCols)
                  newVals =
                    if pkName `elem` insertCols
                      then
                        -- If user provided pk col, we re-insert it in front
                        pkVal : (map snd (filter ((/= pkName) . fst) (zip (columnNames r) (otherVals))))
                      else
                        -- If user did not provide pk col at all, just pkVal plus values in order they were given
                        pkVal : otherVals
               in Row pkVal newCols newVals
          )
          insertedRows
      updatedDb = foldr (insertRow tbl) db finalRows
   in (updatedDb, [])
executeQuery (CREATE tbl cols) db =
  let -- Extract just the column names
      allColNames = map fst cols

      -- Ensure there is an "id" column, which will serve as our primary key
      -- If user didn't specify "id", we add it ourselves at the front
      finalCols =
        if VariableName "id" `elem` allColNames
          then allColNames
          else VariableName "id" : allColNames

      -- Our primary key is always "id"
      pk = VariableName "id"

      updatedDb = Database.createTable tbl pk finalCols db
   in (updatedDb, [])

-- Helper that remove a value from a list by index
removeIndex :: Int -> [a] -> [a]
removeIndex i xs = take i xs ++ drop (i + 1) xs

-- Executes a table result query
executeTableResult :: TableResult -> Database -> [Row]
executeTableResult (BasicSelect cols from whereClause orderKeys limitCount) db =
  let baseRows = getRowsFromFrom from db
      aliasMap = buildAliasMap from
      ctx = EvalContext Nothing aliasMap (dbTables db)
      filteredRows = maybe baseRows (\expr -> filter (evaluateWhereExpressionWithCtx expr db ctx) baseRows) whereClause
      orderedRows = applyOrdering orderKeys db ctx filteredRows
      limitedRows = maybe orderedRows (\n -> take n orderedRows) limitCount
      projectedRows = projectColumns cols db ctx limitedRows
   in projectedRows
executeTableResult (ValueTable v) _ =
  let rows = zipWith (\i vals -> Row (IntVal i) (fakeCols (length vals)) vals) [1 ..] v
   in rows
  where
    fakeCols n = [VariableName ("col" ++ show x) | x <- [1 .. n]]

-- Get rows from a FROM clause
getRowsFromFrom :: IntermediaryTable -> Database -> [Row]
getRowsFromFrom (Q.Table (Aliased _ name)) db =
  case lookup name (dbTables db) of
    Just table -> getAllRows table
    Nothing -> error $ "Table not found: " ++ show name
getRowsFromFrom (Join left right expr) db =
  let leftRows = getRowsFromFrom left db
      rightRows = getRowsFromFrom right db
      aliasMap = buildAliasMap (Join left right expr)
      ctx = EvalContext Nothing aliasMap (dbTables db)
   in [combineRows r1 r2 | r1 <- leftRows, r2 <- rightRows, evaluateJoinExpression expr db ctx r1 r2]
  where
    -- Combine rows from two tables into one row
    combineRows (Row pk1 c1 v1) (Row pk2 c2 v2) =
      -- If they share the same PK name, we might just choose one. Otherwise, combine.
      -- Ideally, we should ensure no column name conflicts
      -- Just merge columns:
      let newCols = nub (c1 ++ c2)
          newVals = map (findVal [(c1, v1), (c2, v2)]) newCols
       in -- If multiple tables have the same column, take the first found
          Row pk1 newCols newVals
    findVal rowSets col =
      fromMaybe NilVal (foldr (\(cc, vv) acc -> acc <|> lookup col (zip cc vv)) Nothing rowSets)
    (<|>) = mplus
getRowsFromFrom (TableResult (Aliased _ sub)) db =
  -- Execute the subquery
  executeTableResult sub db

-- Get all rows from a table
getAllRows :: DbTable -> [Row]
getAllRows table =
  let keys = getAllKeysFromBTree (dbPrimaryIndex table)
   in catMaybes [searchBTree (dbPrimaryIndex table) k | k <- keys]

getAllKeysFromBTree :: BTree Value Row -> [Value]
getAllKeysFromBTree (BTree _ root) = getAllKeysFromNode root

getAllKeysFromNode :: BTreeNode Value Row -> [Value]
getAllKeysFromNode (BTreeNode es cs) =
  let myKeys = map fst es
      childKeys = concatMap getAllKeysFromNode cs
   in myKeys ++ childKeys

-- Evaluate conditions with context
evaluateExpressionWithCtx :: Expression -> Database -> EvalContext -> Row -> Value
evaluateExpressionWithCtx (Column (ColumnName tNameM name)) db ctx row =
  case tNameM of
    Just alias ->
      let realTable = resolveTableName ctx alias
          table = fromMaybe (error $ "Table not found: " ++ show alias) (lookup realTable (allTables ctx))
       in if isColumnOfTable name table
            then fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name row)
            else error $ "Column " ++ show name ++ " not in table " ++ show realTable
    Nothing ->
      let table = findTableForColumn ctx name
       in fromMaybe (error $ "Column not found in the row: " ++ show name) (findValueInRow name row)
evaluateExpressionWithCtx (Value v) _ _ _ = v
evaluateExpressionWithCtx (UnaryOp Len e) db ctx row =
  case evaluateExpressionWithCtx e db ctx row of
    StringVal s -> IntVal (length s)
    _ -> error "LENGTH only works on strings"
evaluateExpressionWithCtx (UnaryOp Not e) db ctx row =
  case evaluateExpressionWithCtx e db ctx row of
    BoolVal b -> BoolVal (not b)
    _ -> error "NOT requires a boolean"
evaluateExpressionWithCtx (BinaryOp op e1 e2) db ctx row =
  let v1 = evaluateExpressionWithCtx e1 db ctx row
      v2 = evaluateExpressionWithCtx e2 db ctx row
   in case op of
        Add -> intOp (+) v1 v2
        Sub -> intOp (-) v1 v2
        Mul -> intOp (*) v1 v2
        Div -> intOp (\x y -> if y == 0 then error "Division by zero" else x `div` y) v1 v2
        Mod -> intOp mod v1 v2
        Eq -> BoolVal (v1 == v2)
        Ne -> BoolVal (v1 /= v2)
        Gt -> cmpOp (>) v1 v2
        Ge -> cmpOp (>=) v1 v2
        Lt -> cmpOp (<) v1 v2
        Le -> cmpOp (<=) v1 v2
        And -> boolOp (&&) v1 v2
        Or -> boolOp (||) v1 v2
        Concat -> error "Concat not implemented"
  where
    intOp f (IntVal i1) (IntVal i2) = IntVal (f i1 i2)
    intOp _ _ _ = error "Binary integer operation on non-integers"

    cmpOp f v1 v2 =
      let toD (IntVal i) = fromIntegral i :: Double
          toD (DoubleVal d) = d
          toD _ = error "Comparison on non-numbers"
       in BoolVal (f (toD v1) (toD v2))

    boolOp f (BoolVal b1) (BoolVal b2) = BoolVal (f b1 b2)
    boolOp _ _ _ = error "Boolean operation on non-booleans"

evaluateJoinExpression :: Expression -> Database -> EvalContext -> Row -> Row -> Bool
evaluateJoinExpression expr db ctx r1 r2 = eval expr
  where
    eval (BinaryOp And e1 e2) = eval e1 && eval e2
    eval (BinaryOp Or e1 e2) = eval e1 || eval e2
    eval (BinaryOp op e1 e2) = compareValues op (evaluateWithRows e1) (evaluateWithRows e2)
    eval (UnaryOp Not e) = not $ eval e
    eval _ = error "Invalid join expression"

    evaluateWithRows ex = evaluateExpressionJoinWithCtx ex db ctx r1 r2

evaluateExpressionJoinWithCtx :: Expression -> Database -> EvalContext -> Row -> Row -> Value
evaluateExpressionJoinWithCtx (Column (ColumnName tNameM name)) db ctx r1 r2 =
  case tNameM of
    Just alias ->
      let realTable = resolveTableName ctx alias
          table = fromMaybe (error $ "Table not found: " ++ show alias) (lookup realTable (allTables ctx))
       in if isColumnOfTable name table
            then
              -- Determine if realTable is from left or right
              -- Suppose the first listed table in FROM or JOIN is the "left" (r1) and
              -- the second listed is "right" (r2). If 'alias' matches left table alias, use r1; else use r2.
              if alias == fst (head (aliases ctx))
                then
                  fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r1)
                else
                  fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r2)
            else error $ "Column " ++ show name ++ " not in table " ++ show realTable
    Nothing ->
      let table = findTableForColumn ctx name
          realTableName = dbTableName table
       in if realTableName == snd (head (aliases ctx))
            then fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r1)
            else fromMaybe (error $ "Column not found: " ++ show name) (findValueInRow name r2)
evaluateExpressionJoinWithCtx (Value v) _ _ _ _ = v
evaluateExpressionJoinWithCtx _ _ _ _ _ = error "These expressions not implemented"

-- Evaluate conditions with context
evaluateWhereExpressionWithCtx :: Expression -> Database -> EvalContext -> Row -> Bool
evaluateWhereExpressionWithCtx expr db ctx row = eval expr
  where
    eval (BinaryOp And e1 e2) = eval e1 && eval e2
    eval (BinaryOp Or e1 e2) = eval e1 || eval e2
    eval (BinaryOp op e1 e2) = compareValues op (evaluateExpressionWithCtx e1 db ctx row) (evaluateExpressionWithCtx e2 db ctx row)
    eval (UnaryOp Not e) = not $ eval e
    eval _ = error "Invalid where expression"

resolveTableName :: EvalContext -> VariableName -> VariableName
resolveTableName ctx alias = fromMaybe alias (lookup alias (aliases ctx))

findValueInRow :: VariableName -> Row -> Maybe Value
findValueInRow name (Row _ cols vals) =
  case elemIndex name cols of
    Just i -> Just (vals !! i)
    Nothing -> Nothing

isColumnOfTable :: VariableName -> DbTable -> Bool
isColumnOfTable col t =
  col == dbPrimaryKeyName t || col `elem` dbOtherColumnNames t

findTableForColumn :: EvalContext -> VariableName -> DbTable
findTableForColumn ctx col =
  let candidates = [t | (_, t) <- allTables ctx, isColumnOfTable col t]
   in case candidates of
        [] -> error ("No table found for column " ++ show col)
        [t] -> t
        _ -> error ("Ambiguous column " ++ show col)

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
  _ -> error $ "Invalid comparison or mismatched types in compareValues: " ++ show (op, v1, v2)

-- Apply ORDER BY
applyOrdering :: [OrderKey] -> Database -> EvalContext -> [Row] -> [Row]
applyOrdering [] _ _ rows = rows
applyOrdering keys db ctx rows =
  sortBy (compareRows keys db ctx) rows

-- Compare rows based on order keys
compareRows :: [OrderKey] -> Database -> EvalContext -> Row -> Row -> Ordering
compareRows keys db ctx row1 row2 =
  foldr combineOrderings EQ $ map (compareByKey row1 row2) keys
  where
    compareByKey r1 r2 (Asc expr) =
      compare (evaluateExpressionWithCtx expr db ctx r1) (evaluateExpressionWithCtx expr db ctx r2)
    compareByKey r1 r2 (Desc expr) =
      compare (evaluateExpressionWithCtx expr db ctx r2) (evaluateExpressionWithCtx expr db ctx r1)

    combineOrderings EQ o = o
    combineOrderings o _ = o

-- Project specific columns
projectColumns :: Columns -> Database -> EvalContext -> [Row] -> [Row]
projectColumns AllColumns _ _ rows = rows
projectColumns (Columns cols) db ctx rows =
  map (projectRow cols db ctx) rows

projectRow :: [Aliased Expression] -> Database -> EvalContext -> Row -> Row
projectRow cols db ctx row =
  let projectedVals = map (\c -> evaluateExpressionWithCtx (value c) db ctx row) cols
      projectedNames = zipWith getProjectedName cols projectedVals
      projectedPk = primaryKey row
   in Row projectedPk projectedNames projectedVals

-- Helper: gets the projected column name
-- If the alias is provided, use it
-- Else, if it's a Column, use the column's name.
-- If it's some other expression without alias, fallback to "expr"
getProjectedName :: Aliased Expression -> Value -> VariableName
getProjectedName (Aliased (Just n) _) _ = n
getProjectedName (Aliased Nothing expr) _ =
  case expr of
    Column (ColumnName _ colName) -> colName
    _ -> VariableName "expr"

-- Serialization functions

-- Save database to file
saveDatabase :: FilePath -> Database -> IO ()
saveDatabase fp db = do
  let bs = S.encode db
  BS.writeFile fp bs

-- Load database from file
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
        Database.createTable
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
      (dbAfter, rows) = executeQuery query db''
  assertEqual "Query should return inserted row" [row] rows

testJoin :: Test
testJoin = TestCase $ do
  let db = emptyDatabase
      -- Create students table
      db1 =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "class_id"]
          db
      -- Create classes table
      db2 =
        Database.createTable
          (VariableName "classes")
          (VariableName "id")
          [VariableName "id", VariableName "classname"]
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
          [VariableName "id", VariableName "classname"]
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
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row (IntVal 1) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal 1, StringVal "John", IntVal 95],
          Row (IntVal 2) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal 2, StringVal "Jane", IntVal 85]
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
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows2 =
        [ Row (IntVal 1) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal 1, StringVal "John", IntVal 95],
          Row (IntVal 2) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal 2, StringVal "Jane", IntVal 85]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows2
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            Nothing
            [Desc (Column (ColumnName Nothing (VariableName "grade")))]
            Nothing
      (dbAfter, rows) = executeQuery query db''
  assertEqual
    "First result should have highest grade"
    (IntVal 95)
    (head [v | Row _ cols vals <- rows, (col, v) <- zip cols vals, col == VariableName "grade"])

testProjection :: Test
testProjection = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
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
      (dbAfter, rows) = executeQuery query db''
  assertEqual
    "Projection should only include selected columns"
    [VariableName "student_name"]
    (columnNames $ head rows)

testLimit :: Test
testLimit = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row (IntVal i) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal i, StringVal ("John" ++ show i), IntVal 95]
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
      (dbAfter, rows2) = executeQuery query db''
  assertEqual "Limit should restrict number of results" 3 (length rows2)

testMultipleInserts :: Test
testMultipleInserts = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade"]
          db
      rows =
        [ Row (IntVal i) [VariableName "id", VariableName "name", VariableName "grade"] [IntVal i, StringVal ("Student" ++ show i), IntVal (85 + i)]
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
      (dbAfter, rows2) = executeQuery query db''
  assertEqual "Should return all 10 inserted rows" 10 (length rows2)

testComplexWhere :: Test
testComplexWhere = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"]
          db
      rows =
        [ Row (IntVal 1) [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"] [IntVal 1, StringVal "John", IntVal 95, IntVal 20],
          Row (IntVal 2) [VariableName "id", VariableName "name", VariableName "grade", VariableName "age"] [IntVal 2, StringVal "Jane", IntVal 85, IntVal 19]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows
      -- Change the condition so that only 'John' matches:
      -- grade > 90 AND age < 21 matches John (95,20) but also Jane doesn't match since 85 not > 90
      whereClause =
        BinaryOp
          And
          (BinaryOp Gt (Column (ColumnName Nothing (VariableName "grade"))) (Value (IntVal 90)))
          (BinaryOp Lt (Column (ColumnName Nothing (VariableName "age"))) (Value (IntVal 21)))
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            (Just whereClause)
            []
            Nothing
      (dbAfter, rows2) = executeQuery query db''
  assertEqual "Complex where should return correct number of rows" 1 (length rows2)

testMultipleOrderBy :: Test
testMultipleOrderBy = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "grade", VariableName "age"]
          db
      rows2 =
        [ Row (IntVal 1) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 1, IntVal 90, IntVal 20],
          Row (IntVal 2) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 2, IntVal 90, IntVal 19],
          Row (IntVal 3) [VariableName "id", VariableName "grade", VariableName "age"] [IntVal 3, IntVal 85, IntVal 21]
        ]
      db'' = foldr (insertRow (VariableName "students")) db' rows2
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
      (dbAfter, rows) = executeQuery query db''
  assertEqual
    "First result should be id=2"
    (IntVal 2)
    (primaryKey $ head rows)

testJoinWithWhere :: Test
testJoinWithWhere = TestCase $ do
  let db = emptyDatabase
      db1 =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "class_id"]
          db
      db2 =
        Database.createTable
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
        Database.createTable
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
      (dbAfter, rows) = executeQuery query db''
  assertEqual
    "Projection should have correct column names"
    [VariableName "full_name", VariableName "surname"]
    (columnNames $ head rows)

testEmptyTableQuery :: Test
testEmptyTableQuery = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
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
      (dbAfter, rows) = executeQuery query db'
  assertEqual "Query on empty table should return empty list" [] rows

testTableAlias :: Test
testTableAlias = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
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
      (dbAfter, rows) = executeQuery query db''
  assertEqual "Query with table alias should return correct result" [row] rows

testMultipleConditionsWhere :: Test
testMultipleConditionsWhere = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "students")
          (VariableName "id")
          [VariableName "id", VariableName "grade", VariableName "attendance"]
          db
      rows =
        [ Row (IntVal 1) [VariableName "id", VariableName "grade", VariableName "attendance"] [IntVal 1, IntVal 95, IntVal 100],
          Row (IntVal 2) [VariableName "id", VariableName "grade", VariableName "attendance"] [IntVal 2, IntVal 85, IntVal 90],
          Row (IntVal 3) [VariableName "id", VariableName "grade", VariableName "attendance"] [IntVal 3, IntVal 92, IntVal 95]
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
      -- Check which rows match:
      -- Row 1: grade=95>90 true; attendance=100 or grade<93 => attendance=100 true => row1 matches
      -- Row 3: grade=92>90 true; attendance=95=100? no; grade<93? yes 92<93 => matches
      -- So actually 2 rows match. Adjust expected result:
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "students"))
            (Just whereClause)
            []
            Nothing
      (dbAfter, rows2) = executeQuery query db''
  assertEqual "Complex where conditions should return correct rows" 2 (length rows2)

testLimitOffset :: Test
testLimitOffset = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "numbers")
          (VariableName "id")
          [VariableName "id", VariableName "value"]
          db
      rows2 =
        [ Row (IntVal i) [VariableName "id", VariableName "value"] [IntVal i, IntVal (i * 10)]
          | i <- [1 .. 5]
        ]
      db'' = foldr (insertRow (VariableName "numbers")) db' rows2
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "numbers"))
            Nothing
            [Asc (Column (ColumnName Nothing (VariableName "id")))]
            (Just 3)
      (dbAfter, rows) = executeQuery query db''
  assertEqual "Should return exactly 3 rows" 3 (length rows)
  assertEqual
    "First row should have id 1"
    (IntVal 1)
    (primaryKey $ head rows)

testSelfJoin :: Test
testSelfJoin = TestCase $ do
  let db = emptyDatabase
      db' =
        Database.createTable
          (VariableName "employees")
          (VariableName "id")
          [VariableName "id", VariableName "name", VariableName "manager_id"]
          db
      rows =
        [ Row (IntVal 1) [VariableName "id", VariableName "name", VariableName "manager_id"] [IntVal 1, StringVal "Boss", IntVal 1],
          Row (IntVal 2) [VariableName "id", VariableName "name", VariableName "manager_id"] [IntVal 2, StringVal "Worker", IntVal 1]
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
      (dbAfter, rows2) = executeQuery query db''
  -- Should return a pair for the boss (managing himself) and the worker (managed by boss), total 2
  assertEqual "Self join should return correct number of rows" 2 (length rows2)

testCreateAndSelect :: Test
testCreateAndSelect = TestCase $ do
  let db = emptyDatabase
  let (dbAfterCreate, _) = executeQuery (CREATE (VariableName "test_table") [(VariableName "a", Q.IntType), (VariableName "b", Q.StringType)]) db
  -- After create, table should exist, but empty
  let (finalDb, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "test_table"))) Nothing [] Nothing)) dbAfterCreate
  assertEqual "Select from empty created table" [] rows

testInsertAndSelect :: Test
testInsertAndSelect = TestCase $ do
  let db = emptyDatabase
  let (dbAfterCreate, _) = executeQuery (CREATE (VariableName "people") [(VariableName "id", Q.IntType), (VariableName "name", Q.StringType), (VariableName "grade", Q.IntType)]) db
  let rowValues = ValueTable [[IntVal 1, StringVal "Arman", IntVal 95]]
  let insertQuery = INSERT (VariableName "people") [VariableName "id", VariableName "name", VariableName "grade"] rowValues
  let (dbAfterInsert, _) = executeQuery insertQuery dbAfterCreate
  let (finalDb, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "people"))) Nothing [] Nothing)) dbAfterInsert
  assertEqual "Select inserted row" 1 (length rows)
  let Row _ cols vals = head rows
  assertEqual "Inserted name" (StringVal "Arman") (vals !! (fromMaybe (error "no name col") (elemIndex (VariableName "name") cols)))
  assertEqual "Inserted grade" (IntVal 95) (vals !! (fromMaybe (error "no grade col") (elemIndex (VariableName "grade") cols)))

testInsertMultiple :: Test
testInsertMultiple = TestCase $ do
  let db = emptyDatabase
  let (dbC, _) = executeQuery (CREATE (VariableName "classroom") [(VariableName "id", Q.IntType), (VariableName "student", Q.StringType), (VariableName "score", Q.IntType)]) db
  let rowValues =
        ValueTable
          [ [IntVal 1, StringVal "Rohan", IntVal 85],
            [IntVal 2, StringVal "Vincent", IntVal 90]
          ]
  let insertQuery = INSERT (VariableName "classroom") [VariableName "id", VariableName "student", VariableName "score"] rowValues
  let (dbI, _) = executeQuery insertQuery dbC
  let (_, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "classroom"))) Nothing [] Nothing)) dbI
  assertEqual "Select after multiple inserts" 2 (length rows)

testCreateInsertAndSelectWithWhere :: Test
testCreateInsertAndSelectWithWhere = TestCase $ do
  let db = emptyDatabase
  let (dbC, _) = executeQuery (CREATE (VariableName "tests") [(VariableName "id", Q.IntType), (VariableName "name", Q.StringType), (VariableName "score", Q.IntType)]) db
  let valTab =
        ValueTable
          [ [IntVal 1, StringVal "Stephanie", IntVal 92],
            [IntVal 2, StringVal "Swap", IntVal 88]
          ]
  let insQ = INSERT (VariableName "tests") [VariableName "id", VariableName "name", VariableName "score"] valTab
  let (dbI, _) = executeQuery insQ dbC
  let whereExpr = BinaryOp Gt (Column (ColumnName Nothing (VariableName "score"))) (Value (IntVal 90))
  let selQ = SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "tests"))) (Just whereExpr) [] Nothing)
  let (_, rows) = executeQuery selQ dbI
  assertEqual "Select with where returns correct rows" 1 (length rows)
  let Row _ cols vals = head rows
  assertEqual "Name should be Stephanie" (StringVal "Stephanie") (vals !! (fromMaybe (error "no name") (elemIndex (VariableName "name") cols)))

testInsertFromSelect :: Test
testInsertFromSelect = TestCase $ do
  let db = emptyDatabase
  let (dbC1, _) = executeQuery (CREATE (VariableName "source_table") [(VariableName "id", Q.IntType), (VariableName "x", Q.IntType)]) db
  let valTab =
        ValueTable
          [ [IntVal 1, IntVal 10],
            [IntVal 2, IntVal 20],
            [IntVal 3, IntVal 30]
          ]
  let (dbI1, _) = executeQuery (INSERT (VariableName "source_table") [VariableName "id", VariableName "x"] valTab) dbC1
  let (dbC2, _) = executeQuery (CREATE (VariableName "dest_table") [(VariableName "id", Q.IntType), (VariableName "y", Q.IntType)]) dbI1
  let subSelect = BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "source_table"))) Nothing [] Nothing
  let insertQ = INSERT (VariableName "dest_table") [VariableName "id", VariableName "y"] subSelect
  let (dbI2, _) = executeQuery insertQ dbC2
  let (_, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "dest_table"))) Nothing [] Nothing)) dbI2
  assertEqual "All rows inserted from select" 3 (length rows)

--------------------------------------------------------------------------------
-- QuickCheck Properties
--------------------------------------------------------------------------------

prop_insertAndQueryByPK :: Row -> Property
prop_insertAndQueryByPK row =
  let db =
        Database.createTable
          (VariableName "test")
          (VariableName "id")
          ((VariableName "id") : columnNames row)
          emptyDatabase
      db' = insertRow (VariableName "test") row db
      whereClause =
        BinaryOp
          Eq
          (Column (ColumnName Nothing (VariableName "id")))
          (Value (primaryKey row))
      query =
        SELECT $
          BasicSelect
            AllColumns
            (Q.Table $ Aliased Nothing (VariableName "test"))
            (Just whereClause)
            []
            Nothing
      (dbAfter, rows) = executeQuery query db'
   in property (rows == [row])

prop_orderByMaintainsOrder :: [Row] -> Property
prop_orderByMaintainsOrder rows =
  not (null rows) ==>
    let cols = columnNames (head rows)
        db =
          Database.createTable
            (VariableName "test")
            (VariableName "id")
            (VariableName "id" : cols)
            emptyDatabase
        db' = foldr (insertRow (VariableName "test")) db rows
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              Nothing
              [Asc (Column (ColumnName Nothing (VariableName "id")))]
              Nothing
        (dbAfter, rows) = executeQuery query db'
     in and $ zipWith (\r1 r2 -> primaryKey r1 <= primaryKey r2) rows (drop 1 rows)

prop_limitConstrainsResults :: [Row] -> Positive Int -> Property
prop_limitConstrainsResults rows (Positive n) =
  not (null rows) ==>
    let cols = columnNames (head rows)
        db =
          Database.createTable
            (VariableName "test")
            (VariableName "id")
            (VariableName "id" : cols)
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
     in length result <= n

prop_whereEqualsFiltersCorrectly :: [Row] -> Property
prop_whereEqualsFiltersCorrectly rows =
  not (null rows) ==>
    let targetRow = head rows
        targetPK = primaryKey targetRow
        cols = columnNames targetRow
        db =
          Database.createTable
            (VariableName "test")
            (VariableName "id")
            ((VariableName "id") : cols)
            emptyDatabase
        db' = foldr (insertRow (VariableName "test")) db rows
        whereClause =
          BinaryOp
            Eq
            (Column (ColumnName Nothing (VariableName "id")))
            (Value targetPK)
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              (Just whereClause)
              []
              Nothing
        (dbAfter, rows) = executeQuery query db'
     in all (\r -> primaryKey r == targetPK) rows

prop_insertOrderIndependent :: [Row] -> Property
prop_insertOrderIndependent rows =
  not (null rows) ==>
    -- Make sure all rows have same columns
    let uniformCols = columnNames (head rows)
        uniformRows = map (\(Row pk _ vals) -> Row pk uniformCols (take (length uniformCols) vals)) rows
        db =
          Database.createTable
            (VariableName "test")
            (VariableName "id")
            (VariableName "id" : uniformCols)
            emptyDatabase
        db1 = foldr (insertRow (VariableName "test")) db uniformRows
        db2 = foldr (insertRow (VariableName "test")) db (reverse uniformRows)
        query =
          SELECT $
            BasicSelect
              AllColumns
              (Q.Table $ Aliased Nothing (VariableName "test"))
              Nothing
              [Asc (Column (ColumnName Nothing (VariableName "id")))]
              Nothing
        result1 = executeQuery query db1
        result2 = executeQuery query db2
     in result1 == result2

prop_createDropCreate :: VariableName -> [VariableName] -> Property
prop_createDropCreate tableName columns =
  not (null columns) ==>
    let columns' = nub columns
        db1 = Database.createTable tableName (head columns') columns' emptyDatabase
        db2 = Database.createTable tableName (head columns') columns' emptyDatabase
     in db1 == db2

-- Serialize a BTree and deserialize it, should be the same
prop_serializeDeserializeBTree :: [(Int, String)] -> Property
prop_serializeDeserializeBTree kvs =
  let uniqueKvs = nub kvs
      bt = foldr insertKV (emptyBTreeIntString 2) uniqueKvs
      encoded = S.encode bt
   in S.decode encoded === Right bt

insertKV :: (Ord k) => (k, v) -> BTree k v -> BTree k v
insertKV (k, v) bt = insertBTree k v bt

emptyBTreeIntString :: Int -> BTree Int String
emptyBTreeIntString t = BTree t (BTreeNode [] [])

-- After create and insert, select returns inserted rows
prop_createInsertSelect :: [(String, Int)] -> Property
prop_createInsertSelect kvs =
  not (null kvs) ==>
    let db = emptyDatabase
        (dbC, _) = executeQuery (CREATE (VariableName "tbl") [(VariableName "name", Q.StringType), (VariableName "grade", Q.IntType)]) db
        valTab = ValueTable (map (\(n, g) -> [StringVal n, IntVal g]) kvs)
        (dbI, _) = executeQuery (INSERT (VariableName "tbl") [VariableName "name", VariableName "grade"] valTab) dbC
        (_, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "tbl"))) Nothing [] Nothing)) dbI
     in length rows == length kvs

-- Insert multiple distinct keys and ensure they appear in select
prop_insertMultipleDistinct :: [Int] -> Property
prop_insertMultipleDistinct xs =
  not (null xs) ==>
    let uniqueXs = nub xs
        db = emptyDatabase
        (dbC, _) = executeQuery (CREATE (VariableName "nums") [(VariableName "num", Q.IntType)]) db
        valTab = ValueTable (map (\i -> [IntVal i]) uniqueXs)
        (dbI, _) = executeQuery (INSERT (VariableName "nums") [VariableName "num"] valTab) dbC
        (_, rows) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "nums"))) Nothing [] Nothing)) dbI
     in length rows == length uniqueXs

-- Create multiple tables and insert into both, ensure queries return correct data
prop_twoTables :: [(String, Int)] -> [(String, Int)] -> Property
prop_twoTables kvs1 kvs2 =
  not (null kvs1) && not (null kvs2) ==>
    let db = emptyDatabase
        (dbC1, _) = executeQuery (CREATE (VariableName "tblA") [(VariableName "name:q", Q.StringType), (VariableName "val", Q.IntType)]) db
        (dbC2, _) = executeQuery (CREATE (VariableName "tblB") [(VariableName "name", Q.StringType), (VariableName "score", Q.IntType)]) dbC1
        valTabA = ValueTable (map (\(n, v) -> [StringVal n, IntVal v]) kvs1)
        valTabB = ValueTable (map (\(n, v) -> [StringVal n, IntVal v]) kvs2)
        (dbI1, _) = executeQuery (INSERT (VariableName "tblA") [VariableName "name", VariableName "val"] valTabA) dbC2
        (dbI2, _) = executeQuery (INSERT (VariableName "tblB") [VariableName "name", VariableName "score"] valTabB) dbI1
        (_, rowsA) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "tblA"))) Nothing [] Nothing)) dbI2
        (_, rowsB) = executeQuery (SELECT (BasicSelect AllColumns (Q.Table (Aliased Nothing (VariableName "tblB"))) Nothing [] Nothing)) dbI2
     in (length rowsA == length kvs1) && (length rowsB == length kvs2)

runTests :: IO ()
runTests = do
  putStrLn "Running unit tests:"
  _ <-
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
          testSelfJoin,
          testCreateAndSelect,
          testInsertAndSelect,
          testInsertMultiple,
          testCreateInsertAndSelectWithWhere,
          testInsertFromSelect
        ]
  putStrLn "Testing insert and query by PK"
  quickCheck prop_insertAndQueryByPK
  putStrLn "Testing limit constrains results"
  quickCheck prop_limitConstrainsResults
  putStrLn "Testing create, drop, create (just comparing create results)"
  quickCheck prop_createDropCreate
  putStrLn "Testing BTree serialization and deserialization"
  quickCheck prop_serializeDeserializeBTree
  putStrLn "Testing create, insert, select"
  quickCheck prop_createInsertSelect
  putStrLn "Testing insert multiple distinct"
  quickCheck prop_insertMultipleDistinct
  putStrLn "Testing two tables"
  quickCheck prop_twoTables
