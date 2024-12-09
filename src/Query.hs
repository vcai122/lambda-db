import Control.Monad (void)
import Data.Char (isSpace)
import Data.Char qualified as Char
import Data.Kind (Type)
import Data.List (elemIndex)
import Data.List qualified as List
import Data.Maybe qualified as Maybe
import Test.HUnit (Assertion, Counts, Test (..), assert, runTestTT, (~:), (~?=))
import Test.QuickCheck (Arbitrary (..), Gen, Property, (==>))
import Test.QuickCheck qualified as QC
import Text.Parsec qualified as P
import Text.Read (readMaybe)

class Pretty a where
  prettyPrint :: a -> String

newtype VariableName = VariableName String
  deriving (Show, Eq)

instance Arbitrary VariableName where
  arbitrary = do
    c <- QC.elements (['a' .. 'z'] ++ ['A' .. 'Z'])
    rest <- QC.listOf (QC.elements (['a' .. 'z'] ++ ['A' .. 'Z'] ++ ['0' .. '9']))
    return $ VariableName (c : rest)
  shrink :: VariableName -> [VariableName]
  shrink (VariableName s) = VariableName <$> filter (not . null) (shrink s)

instance Pretty VariableName where
  prettyPrint (VariableName s) = s

data BinaryOp
  = Add -- `+`  :: Int -> Int -> Int
  | Sub -- `-`  :: Int -> Int -> Int
  | Mul -- `*`  :: Int -> Int -> Int
  | Div -- `//` :: Int -> Int -> Int   -- floor division
  | Mod -- `%`  :: Int -> Int -> Int   -- modulo
  | And -- `&&` :: Bool -> Bool -> Bool
  | Or -- `||` :: Bool -> Bool -> Bool
  | Eq -- `=` :: a -> a -> Bool
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
  deriving (Show, Enum, Bounded, Eq)

instance Arbitrary BinaryOp where
  arbitrary = QC.elements [minBound .. maxBound]

instance Pretty BinaryOp where
  prettyPrint Add = "+"
  prettyPrint Sub = "-"
  prettyPrint Mul = "*"
  prettyPrint Div = "/"
  prettyPrint Mod = "%"
  prettyPrint And = "AND"
  prettyPrint Or = "OR"
  prettyPrint Eq = "="
  prettyPrint Ne = "!="
  prettyPrint Gt = ">"
  prettyPrint Ge = ">="
  prettyPrint Lt = "<"
  prettyPrint Le = "<="
  prettyPrint Concat = "CONCAT"

data UnaryOp
  = Not -- :: a -> Bool
  | Len -- :: String -> Int / Table -> Int
  deriving (Show, Enum, Bounded, Eq)

instance Arbitrary UnaryOp where
  arbitrary = QC.elements [minBound .. maxBound]

instance Pretty UnaryOp where
  prettyPrint Not = "NOT"
  prettyPrint Len = "LENGTH"

data Value
  = NilVal -- nil
  | IntVal Int -- 1
  | DoubleVal Double -- 1.0
  | BoolVal Bool -- false, true
  | StringVal String -- "abd"
  deriving (Show, Eq)

instance Arbitrary Value where
  arbitrary = QC.oneof [pure NilVal, IntVal <$> arbitrary, DoubleVal <$> arbitrary, BoolVal <$> arbitrary, StringVal <$> safeString]
    where
      safeString = filter (/= '"') <$> arbitrary
  shrink NilVal = []
  shrink (IntVal i) = IntVal <$> shrink i
  shrink (DoubleVal d) = DoubleVal <$> shrink d
  shrink (BoolVal b) = BoolVal <$> shrink b
  shrink (StringVal s) = StringVal <$> shrink s

instance Pretty Value where
  prettyPrint NilVal = "NULL"
  prettyPrint (IntVal i) = show i
  prettyPrint (DoubleVal d) = show d
  prettyPrint (BoolVal True) = "TRUE"
  prettyPrint (BoolVal False) = "FALSE"
  prettyPrint (StringVal s) = "\"" ++ s ++ "\""

data Aliased a = Aliased
  { name :: Maybe VariableName,
    value :: a
  }
  deriving (Show, Eq)

instance (Arbitrary a) => Arbitrary (Aliased a) where
  arbitrary = Aliased <$> QC.liftArbitrary arbitrary <*> arbitrary
  shrink (Aliased n v) = case shrink v of
    [] -> Aliased <$> shrink n <*> pure v
    lst -> Aliased n <$> lst

instance (Pretty a) => Pretty (Aliased a) where
  prettyPrint (Aliased Nothing v) = prettyPrint v
  prettyPrint (Aliased (Just n) v) = prettyPrint v ++ " AS " ++ prettyPrint n

data ColumnName = ColumnName {tableName :: Maybe VariableName, columnName :: VariableName}
  deriving (Show, Eq)

instance Arbitrary ColumnName where
  arbitrary = ColumnName <$> arbitrary <*> arbitrary
  shrink (ColumnName t c) = case shrink c of
    [] -> ColumnName <$> shrink t <*> pure c
    lst -> ColumnName t <$> lst

instance Pretty ColumnName where
  prettyPrint (ColumnName Nothing c) = prettyPrint c
  prettyPrint (ColumnName (Just t) c) = prettyPrint t ++ "." ++ prettyPrint c

data Expression
  = Column ColumnName
  | Value Value
  | BinaryOp BinaryOp Expression Expression
  | UnaryOp UnaryOp Expression
  deriving (Show, Eq)

instance Arbitrary Expression where
  arbitrary = QC.oneof [Column <$> arbitrary, Value <$> arbitrary, BinaryOp <$> arbitrary <*> arbitrary <*> arbitrary, UnaryOp <$> arbitrary <*> arbitrary]
  shrink (Column c) = Column <$> shrink c
  shrink (Value v) = Value <$> shrink v
  shrink (BinaryOp op e1 e2) = BinaryOp op <$> shrink e1 <*> shrink e2
  shrink (UnaryOp op e) = UnaryOp op <$> shrink e

instance Pretty Expression where
  prettyPrint (Column c) = prettyPrint c
  prettyPrint (Value v) = prettyPrint v
  prettyPrint (BinaryOp Concat e1 e2) = "CONCAT(" ++ prettyPrint e1 ++ ", " ++ prettyPrint e2 ++ ")"
  prettyPrint (BinaryOp op e1 e2) = "(" ++ prettyPrint e1 ++ " " ++ prettyPrint op ++ " " ++ prettyPrint e2 ++ ")"
  prettyPrint (UnaryOp op e) = prettyPrint op ++ "(" ++ prettyPrint e ++ ")"

concatOfList :: [Expression] -> Expression
concatOfList = List.foldl1 (BinaryOp Concat)

data Columns
  = AllColumns
  | Columns [Aliased Expression] -- invariants its nonempty, but keeping it simpler as list type (ik bad)
  deriving (Show, Eq)

instance Arbitrary Columns where
  arbitrary = QC.frequency [(1, pure AllColumns), (9, Columns <$> QC.listOf1 arbitrary)]
  shrink AllColumns = []
  shrink (Columns lst) = Columns <$> filter (not . null) (shrink lst)

instance Pretty Columns where
  prettyPrint AllColumns = "*"
  prettyPrint (Columns lst) = List.intercalate ", " (prettyPrint <$> lst)

data OrderKey = Asc Expression | Desc Expression
  deriving (Show, Eq)

instance Arbitrary OrderKey where
  arbitrary = QC.oneof [Asc <$> arbitrary, Desc <$> arbitrary]
  shrink (Asc e) = Asc <$> shrink e
  shrink (Desc e) = Desc <$> shrink e

instance Pretty OrderKey where
  prettyPrint (Asc e) = prettyPrint e ++ " ASC"
  prettyPrint (Desc e) = prettyPrint e ++ " DESC"

data IntermediaryTable
  = Table (Aliased VariableName)
  | TableResult (Aliased TableResult)
  | Join
      { left :: IntermediaryTable,
        right :: IntermediaryTable,
        on :: Expression
      }
  deriving (Show, Eq)

instance Arbitrary IntermediaryTable where
  arbitrary = QC.frequency [(5, Table <$> arbitrary), (1, TableResult <$> arbitrary), (2, Join <$> arbitrary <*> arbitrary <*> arbitrary)]
  shrink (Table t) = Table <$> shrink t
  shrink (TableResult t) = TableResult <$> shrink t
  shrink (Join l r e) =
    l
      : r
      : ( case shrink l of
            [] -> case shrink r of
              [] -> Join l r <$> shrink e
              r' -> Join l <$> r' <*> pure e
            l' -> Join <$> l' <*> pure r <*> pure e
        )

instance Pretty IntermediaryTable where
  prettyPrint (Table t) = prettyPrint t
  prettyPrint (TableResult t) = prettyPrint t
  prettyPrint (Join l r e) = "(" ++ prettyPrint l ++ " JOIN " ++ prettyPrint r ++ " ON " ++ prettyPrint e ++ ")"

data TableResult
  = BasicSelect
  { select :: Columns,
    from :: IntermediaryTable,
    whre :: Maybe Expression,
    orderBy :: [OrderKey],
    limit :: Maybe Int
  }
  {-
  -- | Union (TableResult, TableResult)
  -- | Intersect (TableResult, TableResult)
  -- | Except (TableResult, TableResult)
  -}
  deriving (Show, Eq)

instance Arbitrary TableResult where
  arbitrary = BasicSelect <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
  shrink (BasicSelect s f w o l) = case shrink s of
    [] -> case shrink f of
      [] -> case shrink w of
        [] -> case shrink o of
          [] -> BasicSelect s f w o <$> shrink l
          o' -> BasicSelect s f w <$> o' <*> pure l
        w' -> BasicSelect s f <$> w' <*> pure o <*> pure l
      f' -> BasicSelect s <$> f' <*> pure w <*> pure o <*> pure l
    s' -> BasicSelect <$> s' <*> pure f <*> pure w <*> pure o <*> pure l

instance Pretty TableResult where
  prettyPrint (BasicSelect s f w o l) =
    "("
      ++ ("SELECT " ++ prettyPrint s)
      ++ (" FROM " ++ prettyPrint f)
      ++ maybe "" ((" WHERE " ++) . prettyPrint) w
      ++ (if null o then "" else " ORDER BY " ++ List.intercalate ", " (prettyPrint <$> o))
      ++ maybe "" ((" LIMIT " ++) . show) l
      ++ ")"

data Query
  = SELECT TableResult
  {-
  -- | INSERT
  -- | UPDATE
  -- | DELETE
  -}
  deriving (Show, Eq)

instance Arbitrary Query where
  arbitrary = SELECT <$> arbitrary
  shrink (SELECT t) = SELECT <$> shrink t

instance Pretty Query where
  prettyPrint (SELECT t) = prettyPrint t

type Parser a = P.Parsec String () a

caseInsensitiveStringP :: String -> Parser String
caseInsensitiveStringP s = P.try (doIt <* P.spaces)
  where
    doIt = do
      chars <- P.count (length s) P.anyChar
      -- P.lookAhead (P.satisfy isWordStopChar) *> pure () P.<|> P.eof
      P.choice [P.eof, P.lookAhead $ void (P.satisfy isWordStopChar)]
      if map Char.toLower chars == map Char.toLower s
        then return chars
        else fail ("looking for\n" ++ s ++ "\nbut got\n" ++ chars)

-- else fail ("looking for\n" ++ s ++ "\nbut got\n" ++ nextWord)

-- >>> P.parse (P.optionMaybe (caseInsensitiveStringP "AS")) "" "s"
-- Right Nothing

-- >>> P.parse (P.choice [caseInsensitiveStringP "SELECT", wordP]) "" "selecta"
-- Right "selecta"

untilP :: (Char -> Bool) -> Parser String
untilP f = P.many1 (P.satisfy (not . f)) <* P.spaces

charP :: Char -> Parser Char
charP c = P.char c <* P.spaces

stringP :: String -> Parser String
stringP s = P.string' s <* P.spaces

isWordStopChar :: Char -> Bool
isWordStopChar c = Char.isSpace c || c `elem` ",()+/-*=<>!"

wordP :: Parser String
wordP = untilP isWordStopChar

parensP :: Parser a -> Parser a
parensP = P.between (charP '(') (charP ')')

intP :: Parser Int
intP = P.try (doIt <* P.spaces)
  where
    doIt = do
      negative <- P.optionMaybe (charP '-')
      num <- P.many1 P.digit
      return $ case negative of
        Just _ -> -read num
        Nothing -> read num

doubleP :: Parser Double
doubleP = P.try (doIt <* P.spaces)
  where
    doIt = do
      negative <- P.optionMaybe (charP '-')
      whole <- P.many1 P.digit
      charP '.'
      decimal <- P.many1 P.digit
      scientific <-
        P.optionMaybe
          ( P.choice [P.char 'e', P.char 'E'] *> do
              sign <- P.option '+' (P.oneOf "+-")
              scientificDigits <- P.many1 P.digit
              return (sign : scientificDigits)
          )
      let numStr = whole ++ "." ++ decimal ++ maybe "" ('e' :) scientific
      let num = read numStr
      return $ case negative of
        Just _ -> -num
        Nothing -> num

sepBy2P :: Parser a -> Parser b -> Parser [a]
sepBy2P p sep = P.try (doIt <* P.spaces)
  where
    doIt = do
      x <- p
      sep
      xs <- P.sepBy1 p sep
      return (x : xs)

queryParser :: Parser Query
queryParser = selectParser
  where
    selectParser = SELECT <$> tableResultParser
    insertParser = undefined
    updateParser = undefined
    deleteParser = undefined

tableResultParser :: Parser TableResult
-- tableResultParser = P.choice [basicSelectParser, unionParser, intersectParser, exceptParser]
tableResultParser = P.choice [basicSelectParser, parensP tableResultParser]
  where
    basicSelectParser =
      BasicSelect
        <$> (caseInsensitiveStringP "SELECT" *> columnsParser)
        <*> (caseInsensitiveStringP "FROM" *> intermediaryTableParser)
        <*> P.optionMaybe (caseInsensitiveStringP "WHERE" *> expressionParser)
        <*> P.option [] (caseInsensitiveStringP "ORDER BY" *> P.sepBy1 orderKeyParser (charP ','))
        <*> P.optionMaybe (caseInsensitiveStringP "LIMIT" *> intP)
    unionParser = undefined
    intersectParser = undefined
    exceptParser = undefined

aliasParser :: Parser a -> Parser (Aliased a)
aliasParser p = do
  name <- p
  Aliased <$> P.optionMaybe (caseInsensitiveStringP "AS" *> (VariableName <$> wordP)) <*> pure name

columnsParser :: Parser Columns
columnsParser = P.choice [allParser, regularParser]
  where
    allParser = charP '*' >> return AllColumns
    regularParser = Columns <$> P.sepBy1 columnParser (charP ',')
    columnParser = aliasParser expressionParser

prop_columnsParser :: Columns -> Bool
prop_columnsParser c = P.parse columnsParser "" (prettyPrint c) == Right c

c = Columns [Aliased {name = Nothing, value = Value NilVal}]

orderKeyParser :: Parser OrderKey
orderKeyParser = do
  expression <- expressionParser
  asc <- P.optionMaybe (caseInsensitiveStringP "ASC")
  desc <- P.optionMaybe (caseInsensitiveStringP "DESC")
  let order = case (asc, desc) of
        (Nothing, Just _) -> Desc
        (Just _, Just _) -> error "Both ASC and DESC"
        _ -> Asc
  return $ order expression

prop_orderKeyParser :: OrderKey -> Bool
prop_orderKeyParser o = P.parse orderKeyParser "" (prettyPrint o) == Right o

intermediaryTableParser :: Parser IntermediaryTable
intermediaryTableParser = makeJoinParser simpleTableParser
  where
    simpleTableParser = P.choice [Table <$> aliasParser (VariableName <$> wordP), TableResult <$> aliasParser (P.try $ parensP tableResultParser), parensP intermediaryTableParser]
    makeJoinParser :: Parser IntermediaryTable -> Parser IntermediaryTable
    makeJoinParser leftParser = do
      left <- leftParser
      join <- P.optionMaybe (caseInsensitiveStringP "JOIN")
      case join of
        Nothing -> return left
        _ -> do
          right <- simpleTableParser
          on <- caseInsensitiveStringP "ON" *> expressionParser
          makeJoinParser $ return (Join left right on)

prop_intermediaryTableParser :: IntermediaryTable -> Bool
prop_intermediaryTableParser t = P.parse intermediaryTableParser "" (prettyPrint t) == Right t

expressionParser :: Parser Expression
expressionParser = orParser
  where
    simpleExpressionParser = P.choice [concatParser, notParser, lenParser, nilParser, doubleParser, intParser, boolParser, stringParser, columNameParser]
    concatParser = concatOfList <$> (caseInsensitiveStringP "CONCAT" *> parensP (sepBy2P expressionParser (charP ',')))
    notParser = UnaryOp Not <$> (caseInsensitiveStringP "NOT" *> parensP expressionParser)
    lenParser = UnaryOp Len <$> (caseInsensitiveStringP "LENGTH" *> parensP expressionParser)
    nilParser = caseInsensitiveStringP "NULL" >> return (Value NilVal)
    doubleParser = Value . DoubleVal <$> doubleP
    intParser = Value . IntVal <$> intP
    boolParser = Value . BoolVal <$> P.choice [caseInsensitiveStringP "TRUE" >> return True, caseInsensitiveStringP "FALSE" >> return False]
    stringParser = Value . StringVal <$> P.between (P.char '"') (charP '"') (P.many (P.noneOf ['"']))
    columNameParser = do
      col <- wordP
      case elemIndex '.' col of
        Just i -> do
          let (table, _dot : column) = splitAt i col
          return $ Column (ColumnName (Just (VariableName table)) (VariableName column))
        Nothing -> return $ Column (ColumnName Nothing (VariableName col))
    multParser = P.chainl1 factorParser multOp
    addParser = P.chainl1 multParser addOp
    comparisonParser = P.chainl1 addParser comparisonOp
    andParser = P.chainl1 comparisonParser andOp
    orParser = P.chainl1 andParser orOp
    factorParser = P.choice [parensP expressionParser, simpleExpressionParser]
    multOp = P.choice [charP '*' >> return (BinaryOp Mul), charP '/' >> return (BinaryOp Div), charP '%' >> return (BinaryOp Mod)]
    addOp = P.choice [charP '+' >> return (BinaryOp Add), charP '-' >> return (BinaryOp Sub)]
    comparisonOp = P.choice [charP '=' >> return (BinaryOp Eq), stringP ">=" >> return (BinaryOp Ge), stringP "<=" >> return (BinaryOp Le), charP '>' >> return (BinaryOp Gt), charP '<' >> return (BinaryOp Lt), stringP "!=" >> return (BinaryOp Ne)]
    andOp = caseInsensitiveStringP "AND" >> return (BinaryOp And)
    orOp = caseInsensitiveStringP "OR" >> return (BinaryOp Or)

prop_expressionParser :: Expression -> Bool
prop_expressionParser e = P.parse expressionParser "" (prettyPrint e) == Right e

parseQuery :: String -> Query
parseQuery s = case P.parse queryParser "" s of
  Left err -> error (show err)
  Right q -> q

test_basic_select :: Test
test_basic_select =
  "SELECT * FROM table1"
    ~: SELECT
      ( BasicSelect
          { select = AllColumns,
            from = Table $ Aliased Nothing (VariableName "table1"),
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
          { select = Columns [Aliased Nothing (Column (ColumnName Nothing $ VariableName "name")), Aliased Nothing (Column (ColumnName Nothing $ VariableName "grade")), Aliased Nothing (Column (ColumnName Nothing $ VariableName "gpa"))],
            from = Table $ Aliased Nothing (VariableName "students"),
            whre = Just $ BinaryOp And (BinaryOp Gt (Column (ColumnName Nothing $ VariableName "grade")) (Value (IntVal 12))) (BinaryOp Gt (Column (ColumnName Nothing $ VariableName "gpa")) (Value (DoubleVal 3.7))),
            orderBy = [Desc (Column (ColumnName Nothing $ VariableName "name"))],
            limit = Just 10
          }
      )
    ~?= parseQuery "SELECT name, grade, gpa FROM students WHERE grade > 12 AND gpa > 3.7 ORDER BY name DESC LIMIT 10"

-- >>> runTestTT $ TestList  [test_basic_select, test_basic_select_complex]
-- Counts {cases = 2, tried = 2, errors = 0, failures = 0}

prop_roundTrip :: Query -> Bool
prop_roundTrip q = parseQuery (prettyPrint q) == q

runAllTests :: IO ()
runAllTests = do
  putStrLn "Running unit tests"
  runTestTT $ TestList [test_basic_select, test_basic_select_complex]
  putStrLn "Running QuickCheck tests"
  putStrLn "Testing Columns parser"
  QC.quickCheck prop_columnsParser
  putStrLn "Testing OrderKey parser"
  QC.quickCheck prop_orderKeyParser
  putStrLn "Testing IntermediaryTable parser"
  QC.quickCheck prop_intermediaryTableParser
  putStrLn "Testing Expression parser"
  QC.quickCheck prop_expressionParser
  putStrLn "Testing round trip"
  QC.quickCheck prop_roundTrip
  putStrLn "All tests passed"
  return ()

-- >>> runAllTests

query =
  "SELECT a, b, c\n\
  \FROM (SELECT * FROM X) AS t1 \n\
  \JOIN table2 AS t2 \n\
  \ON t2.xyz = t1.xyz \n\
  \JOIN (SELECT * FROM (SELECT a FROM Y))\n\
  \ON a = t1.xyz \n\
  \WHERE (t1.xyz=123)\n\
  \ORDER BY a ASC, b DSC\n\
  \LIMIT 100"

parsed = parseQuery query

-- parsed_as_query = prettyPrint
