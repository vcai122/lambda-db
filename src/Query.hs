import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))

data BinaryOp
  = Add -- `+`  :: Int -> Int -> Int
  | Sub -- `-`  :: Int -> Int -> Int
  | Mul -- `*`  :: Int -> Int -> Int
  | Div -- `//` :: Int -> Int -> Int   -- floor division
  | Mod -- `%`  :: Int -> Int -> Int   -- modulo
  | And -- `&&` :: Bool -> Bool -> Bool
  | Or -- `||` :: Bool -> Bool -> Bool
  | Eq -- `==` :: a -> a -> Bool
  | Ne -- `\=` :: a -> a -> Bool
  | Gt -- `>`  :: a -> a -> Bool
  | Ge -- `>=` :: a -> a -> Bool
  | Lt -- `<`  :: a -> a -> Bool
  | Le -- `<=` :: a -> a -> Bool
  | Concat -- `..` :: String -> String -> String
  -- MAYBE IMPLEMENT THESE
  -- \| Like
  -- \| In
  -- \| Between
  deriving (Eq, Show, Enum, Bounded)

data UnaryOp
  = Neg -- `-` :: Int -> Int
  | Not -- `not` :: a -> Bool
  | Len -- `#` :: String -> Int / Table -> Int
  deriving (Eq, Show, Enum, Bounded)

data Value
  = NilVal -- nil
  | IntVal Int -- 1
  | DoubleVal Double -- 1.0
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  deriving (Eq, Show)

data Aliased a = Aliased
  { name :: Maybe String,
    value :: a
  }
  deriving (Eq, Show)

newtype TableName = TableName String
  deriving (Eq, Show)

data ColumnName = ColumnName {tableName :: Maybe TableName, columnName :: String}
  deriving (Eq, Show)

data Expression
  = Column ColumnName
  | Value Value
  | BinaryOp BinaryOp Expression Expression
  | UnaryOp UnaryOp Expression
  deriving (Eq, Show)

data Columns
  = AllColumns
  | Columns [Aliased ColumnName]
  deriving (Eq, Show)

data OrderKey = Asc ColumnName | Desc ColumnName
  deriving (Eq, Show)

data IntermediaryTable
  = Table TableName
  | TableResult TableResult
  | Join
      { left :: IntermediaryTable,
        right :: IntermediaryTable,
        on :: Expression
      }
  deriving (Eq, Show)

data TableResult
  = BasicSelect
      { select :: Columns,
        from :: IntermediaryTable,
        whre :: Maybe Expression,
        orderBy :: [OrderKey],
        limit :: Maybe Int
      }
  | Union (TableResult, TableResult)
  | Intersect (TableResult, TableResult)
  | Except (TableResult, TableResult)
  deriving (Eq, Show)

data Query
  = SELECT TableResult
  | INSERT
  | UPDATE
  | DELETE
  deriving (Eq, Show)

parseQuery :: String -> Query
parseQuery = undefined

test_basic_select :: Test
test_basic_select =
  "SELECT * FROM table1"
    ~: SELECT
      ( BasicSelect
          { select = AllColumns,
            from = Table (TableName "table1"),
            whre = Nothing,
            orderBy = [],
            limit = Nothing
          }
      )
    ~?= parseQuery "SELECT * FROM table1"

test_basic_select_complex :: Test
test_basic_select_complex =
  "SELECT name, grade, gpa FROM students WHERE grade > 12 AND gpa > 3.7 ORDER BY name DESC LIMIT 10"
    ~: SELECT
      ( BasicSelect
          { select = Columns [Aliased Nothing (ColumnName Nothing "name"), Aliased Nothing (ColumnName Nothing "grade"), Aliased Nothing (ColumnName Nothing "gpa")],
            from = Table (TableName "students"),
            whre = Just $ BinaryOp And (BinaryOp Gt (Column (ColumnName Nothing "grade")) (Value (IntVal 12))) (BinaryOp Gt (Column (ColumnName Nothing "gpa")) (Value (DoubleVal 3.7))),
            orderBy = [Desc (ColumnName Nothing "name")],
            limit = Just 10
          }
      )
    ~?= parseQuery "SELECT name, grade, gpa FROM students WHERE grade > 12 AND gpa > 3.7 ORDER BY name DESC LIMIT 10"
