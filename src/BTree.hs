module BTree where

import Block (Block (Block), BlockID)
import Control.Monad (forM_)
import Data.IORef (IORef)
import Data.List (nub)
import Test.HUnit
import Test.QuickCheck
import Test.QuickCheck.Monadic (assert, monadicIO, run)
import Test.QuickCheck.Monadic qualified as QM
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

-- insert then lookup should return value inserted
prop_insertLookup :: Int -> String -> Property
prop_insertLookup k v = monadicIO $ do
  btree <- run $ emptyBTree 2
  run $ insert btree (k, v)
  result <- run $ lookup btree k
  QM.assert (result == Just v)

-- insert then deleting a value should return nothing
prop_insertDeleteLookup :: Int -> String -> Property
prop_insertDeleteLookup k v = monadicIO $ do
  btree <- run $ emptyBTree 2
  run $ insert btree (k, v)
  run $ delete btree k
  result <- run $ lookup btree k
  QM.assert (result == Nothing)

-- inserting a key twice should update the value
prop_duplicateKeyUpdate :: Int -> String -> String -> Property
prop_duplicateKeyUpdate k v1 v2 = monadicIO $ do
  btree <- run $ emptyBTree 2
  run $ insert btree (k, v1)
  run $ insert btree (k, v2)
  result <- run $ lookup btree k
  QM.assert (result == Just v2)

-- deleting a non-existing key should return nothing
prop_deleteNonExistingKey :: Int -> Property
prop_deleteNonExistingKey k = monadicIO $ do
  btree <- run (emptyBTree 2 :: IO (BTree Int String))
  run $ delete btree k
  result <- run $ lookup btree k
  QM.assert (result == Nothing)

-- mutliple inserts and lookups work properly
prop_multipleInsertLookup :: Property
prop_multipleInsertLookup = forAll genUniqueKeyValues $ \kvs -> monadicIO $ do
  btree <- run $ emptyBTree 2
  run $ forM_ kvs $ \(k, v) -> insert btree (k, v)
  results <- run $ mapM (lookup btree . fst) kvs
  let expected = map (Just . snd) kvs
  QM.assert (results == expected)
  where
    genUniqueKeyValues :: Gen [(Int, String)]
    genUniqueKeyValues = do
      keys <- nub <$> listOf arbitrary
      values <- vectorOf (length keys) arbitrary
      return $ zip keys values

-- adding an element does not delete others
prop_addElementDoesNotDeleteOthers :: [(Int, String)] -> (Int, String) -> Property
prop_addElementDoesNotDeleteOthers initialElements newElement =
  length initialElements
    == length uniqueInitialElements
      ==> monadicIO
    $ do
      btree <- run (emptyBTree 2 :: IO (BTree Int String))
      run $ forM_ uniqueInitialElements $ \kv -> insert btree kv
      run $ insert btree newElement
      results <- run $ mapM (\(k, _) -> lookup btree k) uniqueInitialElements
      let expected = map (Just . snd) uniqueInitialElements
      QM.assert (results == expected)
  where
    uniqueInitialElements = nub initialElements