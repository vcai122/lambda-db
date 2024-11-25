module BTree where

import Block (Block (Block), BlockID)
import Data.IORef (IORef)
import Test.HUnit
import Prelude hiding (lookup)

data BTree k v = BTree
  { rootID :: BlockID,
    t :: Int,
    nextBlockIDRef :: IORef BlockID -- to generate unique BlockIDs
  }

type Serializable k v = (Ord k, Show k, Read k, Show v, Read v)

-- inserts a key-value pair into the B-tree.
insert :: (Serializable k v) => BTree k v -> (k, v) -> IO ()
insert = undefined

-- looks up a key in the B-tree, returning the associated value if found.
lookup :: (Serializable k v) => BTree k v -> k -> IO (Maybe v)
lookup = undefined

-- deletes a key from the B-tree.
delete :: (Serializable k v) => BTree k v -> k -> IO ()
delete = undefined

-- splits a full child block during insertion.
splitChild :: (Serializable k v) => BTree k v -> Block k v -> Int -> IO ()
splitChild = undefined

-- inserts a key-value pair into a non-full node.
insertNonFull :: (Serializable k v) => BTree k v -> Block k v -> (k, v) -> IO ()
insertNonFull = undefined

-- merges child blocks during deletion.
mergeChildren :: (Serializable k v) => BTree k v -> Block k v -> Int -> IO ()
mergeChildren = undefined

emptyBTree :: (Serializable k v) => Int -> IO (BTree k v)
emptyBTree = undefined

createSampleBTree :: IO (BTree Int String)
createSampleBTree = do
  btree <- emptyBTree 2
  insert btree (10, "Value10")
  insert btree (20, "Value20")
  insert btree (5, "Value5")
  insert btree (6, "Value6")
  insert btree (12, "Value12")
  insert btree (30, "Value30")
  insert btree (7, "Value7")
  insert btree (17, "Value17")
  return btree

testInsert :: Test
testInsert = TestCase $ do
  btree <- emptyBTree 2
  insert btree (15, "Value15")
  result <- lookup btree 15
  assertEqual "Insert and lookup key 15" (Just "Value15") result

testLookup :: Test
testLookup = TestCase $ do
  btree <- createSampleBTree
  result1 <- lookup btree 6
  result2 <- lookup btree 15
  assertEqual "Lookup existing key 6" (Just "Value6") result1
  assertEqual "Lookup non-existing key 15" Nothing result2
