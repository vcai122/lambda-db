module BTree where

import Data.ByteString qualified as BS
import Data.Serialize qualified as S
import GHC.Generics (Generic)
import System.Directory (createDirectoryIfMissing, doesFileExist)

import Test.HUnit (runTestTT, Test(..), assert, (~:), (~?=), assertEqual)
import Test.QuickCheck
import Data.List (nub)

data BTreeNode k v
  = InternalNode
      { keys :: [k],
        children :: [BTreeNode k v]
      }
  | LeafNode
      { keys :: [k],
        values :: [v]
      }
  deriving (Show, Generic)

instance (S.Serialize k, S.Serialize v) => S.Serialize (BTreeNode k v)

data BTree k v = BTree
  { t :: Int, -- min degree
    root :: BTreeNode k v
  }
  deriving (Show, Generic)

instance (S.Serialize k, S.Serialize v) => S.Serialize (BTree k v)

-- | Search for a key in the B-Tree node; if leaf, returns Just value if found,
-- | otherwise descends to the appropriate child
searchBTreeNode :: (Ord k) => k -> BTreeNode k v -> Maybe v
searchBTreeNode key (LeafNode ks vs) =
  lookup key (zip ks vs)
searchBTreeNode k (InternalNode ks cs) =
  case break (>= k) ks of
    (_, []) -> searchBTreeNode k (last cs)
    (left, x : _) ->
      if k == x
        then Nothing
        else searchBTreeNode k (cs !! length left)

-- | top-level search
searchBTree :: (Ord k) => BTree k v -> k -> Maybe v
searchBTree (BTree _ r) k = searchBTreeNode k r

-- | Insert a key-value pair into the BTree; if root is full, split it before inserting
insertBTree :: (Ord k) => k -> v -> BTree k v -> BTree k v
insertBTree key value (BTree t rootNode)
  | fullNode t rootNode =
      let (midKey, leftNode, rightNode) = splitNode t rootNode
          newRoot = InternalNode [midKey] [leftNode, rightNode]
       in BTree t (insertNonFull t key value newRoot)
  | otherwise =
      BTree t (insertNonFull t key value rootNode)

-- | Insert into a node that is known to be non-full
insertNonFull :: (Ord k) => Int -> k -> v -> BTreeNode k v -> BTreeNode k v
insertNonFull tVal key value (LeafNode ks vs) =
  let (leftKs, rightKs) = break (> key) ks
      (leftVs, rightVs) = splitAt (length leftKs) vs
   in LeafNode (leftKs ++ [key] ++ rightKs) (leftVs ++ [value] ++ rightVs)
insertNonFull tVal key value (InternalNode ks cs) =
  let idx = findChildIndex key ks
      child = cs !! idx
   in if fullNode tVal child
        then
          let (midKey, leftChild, rightChild) = splitNode tVal child
              newKeys = insertKey midKey ks
              newChildren = replaceChildPair cs idx leftChild rightChild
              updatedNode = InternalNode newKeys newChildren
              newIdx = if key < midKey then idx else idx + 1
              updatedChildren = replaceSingleChild newChildren newIdx (insertNonFull tVal key value (newChildren !! newIdx))
           in InternalNode newKeys updatedChildren
        else
          InternalNode ks (replaceSingleChild cs idx (insertNonFull tVal key value child))

-- | Delete a key from the BTree
deleteBTree :: (Ord k) => k -> BTree k v -> BTree k v
deleteBTree key (BTree t rootNode) =
  let newRoot = deleteKey t key rootNode
   in case newRoot of
        InternalNode [] [c] -> BTree t c
        other -> BTree t other

-- | Delete a key from a node
--   THIS IS SIMPLIFIED, BALANCING IS NOT OPTIMAL
--   If the node is a leaf, remove the key if present.
--   If the node is internal and the key is found here, find successor or predecessor and swap then delete from leaf
--   If the key is not found, descend into the correct child node; if a child has too few keys, fix (merge/borrow) before descending
deleteKey :: (Ord k) => Int -> k -> BTreeNode k v -> BTreeNode k v
deleteKey tVal key (LeafNode ks vs) =
  case break (== key) ks of
    (left, _ : right) ->
      let (leftVs, _ : rightVs) = splitAt (length left) vs
       in LeafNode (left ++ right) (leftVs ++ rightVs)
    _ -> LeafNode ks vs
deleteKey tVal key node@(InternalNode ks cs) =
  case break (>= key) ks of
    (left, x : right)
      | x == key ->
          -- key found in internal node, so replace with predecessor or successor
          let idx = length left
           in if isLeaf (cs !! idx)
                then -- just remove from leaf child
                  let LeafNode ksc vsc = deleteKey tVal key (cs !! idx)
                   in InternalNode (left ++ right) (replaceSingleChild cs idx (LeafNode ksc vsc))
                else
                  -- replace with predecessor (max key in left subtree)
                  let (predKey, predValue, newLeftSubtree) = getPredecessor (cs !! idx)
                      -- delete predKey from that subtree
                      newLeftSubtree' = deleteKey tVal predKey newLeftSubtree
                      (rKs, rCs) = (left ++ (predKey : right), replaceSingleChild cs idx newLeftSubtree')
                   in -- val does not reside in internal nodes, so no direct update needed
                      InternalNode rKs rCs
    (_, []) ->
      -- key isn't in this node, go into last child
      let lastChild = last cs
          lastChild' = fixChildUnderflow tVal (deleteKey tVal key lastChild)
       in InternalNode ks (init cs ++ [lastChild'])
    (left, x : right) ->
      -- key isn't equal to x, so must be in child at position length left
      let idx = length left
          targetChild = cs !! idx
          targetChild' = fixChildUnderflow tVal (deleteKey tVal key targetChild)
       in InternalNode ks (replaceSingleChild cs idx targetChild')

-- | get predecessor key from a subtree (go to the rightmost leaf)
getPredecessor :: BTreeNode k v -> (k, v, BTreeNode k v)
getPredecessor (LeafNode ks vs) =
  let k' = last ks
      v' = last vs
      initKs = init ks
      initVs = init vs
   in (k', v', LeafNode initKs initVs)
getPredecessor (InternalNode ks cs) =
  getPredecessor (last cs)

-- | check if node is leaf
isLeaf :: BTreeNode k v -> Bool
isLeaf (LeafNode _ _) = True
isLeaf _ = False

-- | Fix child underflow if needed.
-- TODO: impl
fixChildUnderflow :: Int -> BTreeNode k v -> BTreeNode k v
fixChildUnderflow _ node = node

-- | find the child index to descend into during insert/search
findChildIndex :: (Ord k) => k -> [k] -> Int
findChildIndex key ks = length (takeWhile (< key) ks)

-- | insert a key into a sorted list of keys
insertKey :: (Ord k) => k -> [k] -> [k]
insertKey key ks =
  let (left, right) = break (> key) ks
   in left ++ [key] ++ right

-- | replace a single child at idx with one new child node
replaceSingleChild :: [BTreeNode k v] -> Int -> BTreeNode k v -> [BTreeNode k v]
replaceSingleChild cs idx newChild = take idx cs ++ [newChild] ++ drop (idx + 1) cs

-- | replace a child at idx with two children
replaceChildPair :: [BTreeNode k v] -> Int -> BTreeNode k v -> BTreeNode k v -> [BTreeNode k v]
replaceChildPair cs idx left right = take idx cs ++ [left, right] ++ drop (idx + 1) cs

-- | check if node is full
fullNode :: Int -> BTreeNode k v -> Bool
fullNode tVal (LeafNode ks _) = length ks == 2 * tVal - 1
fullNode tVal (InternalNode ks _) = length ks == 2 * tVal - 1

-- | split full node into two nodes and promote the median key
splitNode :: Int -> BTreeNode k v -> (k, BTreeNode k v, BTreeNode k v)
splitNode tVal (LeafNode ks vs) =
  let mid = tVal - 1
      midKey = ks !! mid
      (leftKs, rightKs) = splitAt mid ks
      (leftVs, rightVs) = splitAt mid vs
      rightKs' = drop 1 rightKs
      rightVs' = drop 1 rightVs
      leftNode = LeafNode leftKs leftVs
      rightNode = LeafNode rightKs' rightVs'
   in (midKey, leftNode, rightNode)
splitNode tVal (InternalNode ks cs) =
  let mid = tVal - 1
      midKey = ks !! mid
      (leftKs, rightKs) = splitAt mid ks
      rightKs' = drop 1 rightKs
      (leftCs, rightCs) = splitAt (mid + 1) cs
      leftNode = InternalNode leftKs leftCs
      rightNode = InternalNode rightKs' rightCs
   in (midKey, leftNode, rightNode)

-- | Save the entire BTree to a file
saveBTree :: (S.Serialize k, S.Serialize v) => FilePath -> BTree k v -> IO ()
saveBTree fp bt = do
  let bs = S.encode bt
  BS.writeFile fp bs

-- | load the entire BTree from a file; if the file doesn't exist, create an empty tree
loadBTree ::
  forall k v.
  (S.Serialize k, S.Serialize v, Ord k) =>
  FilePath ->
  Int ->
  IO (BTree k v)
loadBTree fp config = do
  exists <- doesFileExist fp
  if exists
    then do
      bs <- BS.readFile fp
      case S.decode bs of
        Left err -> error ("Failed to decode BTree file: " ++ err)
        Right bt -> return bt
    else do
      let emptyTree = BTree config (LeafNode [] []) :: BTree k v
      saveBTree fp emptyTree
      return emptyTree

-- tests

emptyBTree :: Int -> BTree k v
emptyBTree t = BTree t (LeafNode [] [])

-- Insert a key-value pair into a B-Tree
insert :: (Ord k) => (k, v) -> BTree k v -> BTree k v
insert (k, v) bt = insertBTree k v bt

-- Lookup a value by key from a B-Tree
lookupValue :: (Ord k) => k -> BTree k v -> Maybe v
lookupValue k bt = searchBTree bt k

-- Create a sample B-Tree
createSampleBTree :: BTree Int String
createSampleBTree =
  let initial = emptyBTree 2
      inserts =
        [ (10, "Value10")
        , (20, "Value20")
        , (5,  "Value5")
        , (6,  "Value6")
        , (12, "Value12")
        , (30, "Value30")
        , (7,  "Value7")
        , (17, "Value17")
        ]
  in foldr insert initial inserts

testInsert :: Test
testInsert = TestCase $ do
  let bt = insert (15, "Value15") (emptyBTree 2)
  let result = lookupValue 15 bt
  assertEqual "Insert and lookup key 15" (Just "Value15") result

-- >>> runTestTT testInsert
-- Counts {cases = 1, tried = 1, errors = 0, failures = 0}

testLookup :: Test
testLookup = TestCase $ do
  let bt = createSampleBTree
  let result1 = lookupValue 6 bt
  let result2 = lookupValue 15 bt
  assertEqual "Lookup existing key 6" (Just "Value6") result1
  assertEqual "Lookup non-existing key 15" Nothing result2

-- >>> runTestTT testLookup
-- Counts {cases = 1, tried = 1, errors = 0, failures = 0}

--------------------------------------------------------------------------------
-- QuickCheck Properties
--------------------------------------------------------------------------------

-- insert then lookup should return the value inserted
prop_insertLookup :: Int -> String -> Bool
prop_insertLookup k v =
  let bt = insert (k, v) (emptyBTree 2)
  in lookupValue k bt == Just v

-- insert then deleting a value should return nothing
prop_insertDeleteLookup :: Int -> String -> Bool
prop_insertDeleteLookup k v =
  let bt = insert (k, v) (emptyBTree 2)
      bt' = deleteBTree k bt
  in lookupValue k bt' == Nothing

-- inserting a key twice should update the value
prop_duplicateKeyUpdate :: Int -> String -> String -> Bool
prop_duplicateKeyUpdate k v1 v2 =
  let bt = insert (k, v1) (emptyBTree 2)
      bt' = insert (k, v2) bt
  in lookupValue k bt' == Just v2

-- deleting a non-existing key should return nothing for that key
prop_deleteNonExistingKey :: Int -> Bool
prop_deleteNonExistingKey k =
  let bt = deleteBTree k (emptyBTree 2 :: BTree Int String)
  in lookupValue k bt == Nothing

-- multiple inserts and lookups
prop_multipleInsertLookup :: [(Int, String)] -> Property
prop_multipleInsertLookup kvs =
  let uniqueKvs = nub kvs
      bt = foldr insert (emptyBTree 2) uniqueKvs
      results = map (\(k,_) -> lookupValue k bt) uniqueKvs
      expected = map (Just . snd) uniqueKvs
  in length kvs == length uniqueKvs ==> results == expected

-- adding an element does not delete others
prop_addElementDoesNotDeleteOthers :: [(Int, String)] -> (Int, String) -> Property
prop_addElementDoesNotDeleteOthers initialElements newElement =
  let uniqueInitialElements = nub initialElements
      btInitial = foldr insert (emptyBTree 2) uniqueInitialElements
      btFinal = insert newElement btInitial
      results = map (\(k, _) -> lookupValue k btFinal) uniqueInitialElements
      expected = map (Just . snd) uniqueInitialElements
  in length initialElements == length uniqueInitialElements ==> results == expected

main :: IO ()
main = do
  _ <- runTestTT $ TestList [testInsert, testLookup]
  quickCheck prop_insertLookup
  quickCheck prop_insertDeleteLookup
  quickCheck prop_duplicateKeyUpdate
  quickCheck prop_deleteNonExistingKey
  quickCheck prop_multipleInsertLookup
  quickCheck prop_addElementDoesNotDeleteOthers
