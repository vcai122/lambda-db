module Block
  ( Block (..),
    BlockID,
    blockInsert,
    blockLookup,
    blockDelete,
    serializeBlock,
    deserializeBlock,
  )
where

import Test.HUnit

type BlockID = Int

data Block k v = Block
  { blockID :: BlockID,
    n :: Int,
    entries :: [(k, v)],
    leaf :: Bool,
    childrenIDs :: Maybe [BlockID] -- should be nothing if leaf
  }
  deriving (Show, Read, Eq)

-- insert a key-value pair into a block, returning an error if the block is full.
blockInsert :: (Ord k) => Block k v -> (k, v) -> Either String (Block k v)
blockInsert = undefined

-- lookup a key in a block and return the associated value if it exists.
blockLookup :: (Ord k) => Block k v -> k -> Maybe v
blockLookup = undefined

-- deletes a key from a block if it exists.
blockDelete :: (Ord k) => Block k v -> k -> Either String (Block k v)
blockDelete = undefined

-- serializes a block to disk at the given file path.
serializeBlock :: (Show k) => Block k v -> FilePath -> IO ()
serializeBlock block filePath = undefined

-- deserializes a block from disk at the given file path.
deserializeBlock :: (Read k) => FilePath -> IO (Block k v)
deserializeBlock filePath = undefined

createSampleBlock :: Block Int String
createSampleBlock =
  Block
    { blockID = 1,
      n = 2,
      entries = [(10, "Ten"), (20, "Twenty")],
      leaf = True,
      childrenIDs = Nothing
    }

testBlockInsert :: Test
testBlockInsert = TestCase $ do
  let block = createSampleBlock
      newEntry = (15, "Fifteen")
      expectedEntries = [(10, "Ten"), (15, "Fifteen"), (20, "Twenty")]
      expectedBlock = block {n = 3, entries = expectedEntries}
      result = blockInsert block newEntry
  case result of
    Left err -> assertFailure ("blockInsert failed with error: " ++ err)
    Right updatedBlock -> assertEqual "Inserting into non-full block" expectedBlock updatedBlock

testBlockInsertFull :: Test
testBlockInsertFull = TestCase $ do
  let block = createSampleBlock {n = 3, entries = [(10, "Ten"), (20, "Twenty"), (30, "Thirty")]}
      newEntry = (25, "Twenty-Five")
      result = blockInsert block newEntry
  case result of
    Left err -> assertEqual "Inserting into full block should fail" "Block is full" err
    Right _ -> assertFailure "Inserting into a full block should not succeed"

testBlockLookup :: Test
testBlockLookup = TestCase $ do
  let block = createSampleBlock
  let result1 = blockLookup block 10
  assertEqual "Lookup existing key 10" (Just "Ten") result1
  let result2 = blockLookup block 15
  assertEqual "Lookup non-existing key 15" Nothing result2

testBlockDelete :: Test
testBlockDelete = TestCase $ do
  let block = createSampleBlock
      expectedEntries = [(20, "Twenty")]
      expectedBlock = block {n = 1, entries = expectedEntries}
      result = blockDelete block 10
  case result of
    Left err -> assertFailure ("blockDelete failed with error: " ++ err)
    Right updatedBlock -> assertEqual "Deleting existing key" expectedBlock updatedBlock
