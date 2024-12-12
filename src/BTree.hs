module BTree where

import Data.ByteString qualified as BS
import Data.List (nub, sort)
import Data.Serialize qualified as S
import GHC.Generics (Generic)
import Test.HUnit
import Test.QuickCheck (Arbitrary (..), Gen, Property, (==>))
import Test.QuickCheck qualified as QC

data BTreeNode k v = BTreeNode
  { entries :: [(k, v)],
    children :: [BTreeNode k v]
  }
  deriving (Show, Generic)

instance (S.Serialize k, S.Serialize v) => S.Serialize (BTreeNode k v)

data BTree k v = BTree
  { t :: Int,
    root :: BTreeNode k v
  }
  deriving (Show, Generic)

instance (S.Serialize k, S.Serialize v) => S.Serialize (BTree k v)

-- SEARCH
searchBTree :: (Ord k) => BTree k v -> k -> Maybe v
searchBTree (BTree _ r) k = searchNode k r

searchNode :: (Ord k) => k -> BTreeNode k v -> Maybe v
searchNode key (BTreeNode es cs) =
  case break ((>= key) . fst) es of
    (left, (xk, xv) : right) | xk == key -> Just xv
    (left, (xk, xv) : right) ->
      let idx = length left
       in if null cs then Nothing else searchNode key (cs !! idx)
    (_, []) ->
      if null cs then Nothing else searchNode key (last cs)

-- INSERTION
insertBTree :: (Ord k) => k -> v -> BTree k v -> BTree k v
insertBTree key value (BTree deg rootNode)
  | fullNode deg rootNode =
      let (midKV, leftNode, rightNode) = splitNode deg rootNode
          newRoot = BTreeNode [midKV] [leftNode, rightNode]
       in BTree deg (insertNonFull deg key value newRoot)
  | otherwise =
      BTree deg (insertNonFull deg key value rootNode)

insertNonFull :: (Ord k) => Int -> k -> v -> BTreeNode k v -> BTreeNode k v
insertNonFull tVal key value node@(BTreeNode es cs)
  | null cs =
      -- leaf insert
      let es' = insertEntry (key, value) es
       in BTreeNode es' cs
  | otherwise =
      let idx = findChildIndex key es
          child = cs !! idx
       in if fullNode tVal child
            then
              let (midKV, leftChild, rightChild) = splitNode tVal child
                  es' = insertEntry midKV es
                  cs' = replaceChildPair cs idx leftChild rightChild
                  newIdx = if key < fst midKV then idx else idx + 1
               in BTreeNode es' (replaceSingleChild cs' newIdx (insertNonFull tVal key value (cs' !! newIdx)))
            else
              BTreeNode es (replaceSingleChild cs idx (insertNonFull tVal key value child))

-- DELETION
deleteBTree :: (Ord k, Eq v) => k -> BTree k v -> BTree k v
deleteBTree key (BTree deg rootNode) =
  let (newRoot, _) = deleteKey deg key rootNode
   in case newRoot of
        BTreeNode [] [c] -> BTree deg c
        other -> BTree deg other

deleteKey :: (Ord k, Eq v) => Int -> k -> BTreeNode k v -> (BTreeNode k v, Bool)
deleteKey tVal key node@(BTreeNode es cs)
  | null cs =
      -- Leaf node deletion
      case break ((== key) . fst) es of
        (_, []) -> (node, False) -- key not found
        (left, _match : right) -> (BTreeNode (left ++ right) cs, True)
  | otherwise =
      case break ((>= key) . fst) es of
        (left, (xk, xv) : right)
          | xk == key ->
              let idx = length left
                  leftChild = cs !! idx
                  rightChild = cs !! (idx + 1)
               in if null (children leftChild)
                    then
                      -- The left child is a leaf, so just get predecessor from there
                      let (pk, pv, newLeftChild) = getPredecessor leftChild
                          newEs = replaceAt idx (pk, pv) es
                          newCs = replaceSingleChild cs idx newLeftChild
                          (fixedNode, _) = fixUnderflow tVal (BTreeNode newEs newCs) idx
                       in (fixedNode, True)
                    else
                      -- Before we call getPredecessor, ensure leftChild has at least t keys
                      let (safeNode, _) = ensureSafeChild tVal (BTreeNode es cs) idx
                          BTreeNode es2 cs2 = safeNode
                          leftChild2 = cs2 !! idx
                          (pk, pv, newLeftChild) = getPredecessor leftChild2
                          newEs = replaceAt idx (pk, pv) es2
                          newCs = replaceSingleChild cs2 idx newLeftChild
                          (fixedNode, _) = fixUnderflow tVal (BTreeNode newEs newCs) idx
                       in (fixedNode, True)
        (_, []) ->
          let lastIndex = length cs - 1
              (child, didDel) = deleteKey tVal key (last cs)
              newChildren = replaceSingleChild cs lastIndex child
           in if didDel
                then fixUnderflow tVal (BTreeNode es newChildren) lastIndex
                else (node, False)
        (left, (xk, xv) : right) ->
          let idx = length left
              (child, didDel) = deleteKey tVal key (cs !! idx)
              newChildren = replaceSingleChild cs idx child
           in if didDel
                then fixUnderflow tVal (BTreeNode es newChildren) idx
                else (node, False)

-- ensure that the child at index idx of node has at least t keys, othewise borrow or merge from siblings
ensureSafeChild :: (Ord k) => Int -> BTreeNode k v -> Int -> (BTreeNode k v, Bool)
ensureSafeChild tVal node@(BTreeNode es cs) idx
  | length (entries (cs !! idx)) >= tVal = (node, False) -- Already safe
  | otherwise =
      let leftSibling = if idx > 0 then Just (cs !! (idx - 1)) else Nothing
          rightSibling = if idx < length cs - 1 then Just (cs !! (idx + 1)) else Nothing
       in case (leftSibling, rightSibling) of
            (Just l, _) | canBorrow tVal l -> (borrowFromLeft tVal idx node, True)
            (_, Just r) | canBorrow tVal r -> (borrowFromRight tVal idx node, True)
            (Just _, _) -> (mergeNodes tVal idx (idx - 1) node, True)
            (_, Just _) -> (mergeNodes tVal idx idx node, True)
            _ ->
              if length cs == 1
                then (head cs, True)
                else error "ensureSafeChild: No siblings to borrow or merge from. Invalid B-tree structure."

-- get the predecessor (largest key) from a subtree
getPredecessor :: BTreeNode k v -> (k, v, BTreeNode k v)
getPredecessor (BTreeNode es cs)
  | null cs =
      let (pk, pv) = last es
       in (pk, pv, BTreeNode (init es) cs)
  | otherwise =
      let (pk, pv, newLastChild) = getPredecessor (last cs)
       in (pk, pv, BTreeNode es (replaceAt (length cs - 1) newLastChild cs))

fixUnderflow :: (Ord k) => Int -> BTreeNode k v -> Int -> (BTreeNode k v, Bool)
fixUnderflow tVal node@(BTreeNode es cs) idx
  | not (nodeUnderflow tVal (cs !! idx)) = (node, True)
  | otherwise =
      let leftSibling = if idx > 0 then Just (cs !! (idx - 1)) else Nothing
          rightSibling = if idx < length cs - 1 then Just (cs !! (idx + 1)) else Nothing
       in case (leftSibling, rightSibling) of
            (Just l, _)
              | canBorrow tVal l ->
                  (borrowFromLeft tVal idx node, True)
            (_, Just r)
              | canBorrow tVal r ->
                  (borrowFromRight tVal idx node, True)
            (Just _, _) ->
              (mergeNodes tVal idx (idx - 1) node, True)
            (_, Just _) ->
              (mergeNodes tVal idx idx node, True)
            _ -> (node, True)

nodeUnderflow :: Int -> BTreeNode k v -> Bool
nodeUnderflow tVal (BTreeNode es _) = length es < tVal - 1

canBorrow :: Int -> BTreeNode k v -> Bool
canBorrow tVal (BTreeNode es _) = length es >= tVal

borrowFromLeft :: (Ord k) => Int -> Int -> BTreeNode k v -> BTreeNode k v
borrowFromLeft _ 0 node = node
borrowFromLeft tVal idx (BTreeNode es cs) =
  let leftNode = cs !! (idx - 1)
      targetNode = cs !! idx
      (lk, lv) = last (entries leftNode)
      midIndex = idx - 1
      (midK, midV) = es !! midIndex
      newLeftEntries = init (entries leftNode)
      newEs = replaceAt midIndex (lk, lv) es
      newTargetEntries = insertEntry (midK, midV) (entries targetNode)
      newLeft = BTreeNode newLeftEntries (children leftNode)
      newTarget = BTreeNode newTargetEntries (children targetNode)
      newChildren = replaceAt (idx - 1) newLeft (replaceAt idx newTarget cs)
   in BTreeNode newEs newChildren

borrowFromRight :: (Ord k) => Int -> Int -> BTreeNode k v -> BTreeNode k v
borrowFromRight tVal idx (BTreeNode es cs) =
  let rightNode = cs !! (idx + 1)
      targetNode = cs !! idx
      (rk, rv) = head (entries rightNode)
      (midK, midV) = es !! idx
      newRightEntries = tail (entries rightNode)
      newEs = replaceAt idx (rk, rv) es
      newTargetEntries = insertEntry (midK, midV) (entries targetNode)
      newRight = BTreeNode newRightEntries (children rightNode)
      newTarget = BTreeNode newTargetEntries (children targetNode)
      newChildren = replaceAt idx newTarget (replaceAt (idx + 1) newRight cs)
   in BTreeNode newEs newChildren

mergeNodes :: Int -> Int -> Int -> BTreeNode k v -> BTreeNode k v
mergeNodes tVal idx leftIdx (BTreeNode es cs) =
  let leftNode = cs !! leftIdx
      rightNode = cs !! (leftIdx + 1)
      (mk, mv) = es !! leftIdx
      newEs = removeAt leftIdx es
      newChildren = removeAt (leftIdx + 1) cs
      mergedEntries = entries leftNode ++ [(mk, mv)] ++ entries rightNode
      mergedNode = BTreeNode mergedEntries (children leftNode ++ children rightNode)
      finalChildren = replaceAt leftIdx mergedNode newChildren
   in BTreeNode newEs finalChildren

-- UTILITY FUNCTIONS
removeAt :: Int -> [a] -> [a]
removeAt i xs = take i xs ++ drop (i + 1) xs

replaceAt :: Int -> a -> [a] -> [a]
replaceAt i x xs = take i xs ++ [x] ++ drop (i + 1) xs

isLeaf :: BTreeNode k v -> Bool
isLeaf (BTreeNode _ cs) = null cs

-- find the child index to descend into
findChildIndex :: (Ord k) => k -> [(k, v)] -> Int
findChildIndex key es = length (takeWhile ((< key) . fst) es)

fullNode :: Int -> BTreeNode k v -> Bool
fullNode tVal (BTreeNode es _) = length es == 2 * tVal - 1

splitNode :: Int -> BTreeNode k v -> ((k, v), BTreeNode k v, BTreeNode k v)
splitNode tVal (BTreeNode es cs) =
  let mid = tVal - 1
      midKV = es !! mid
      (leftEs, rightEs) = splitAt mid es
      rightEs' = tail rightEs
      (leftCs, rightCs) =
        if null cs
          then ([], [])
          else splitAt tVal cs
      leftNode = BTreeNode leftEs leftCs
      rightNode = BTreeNode rightEs' rightCs
   in (midKV, leftNode, rightNode)

-- insert an entry in sorted order by k
insertEntry :: (Ord k) => (k, v) -> [(k, v)] -> [(k, v)]
insertEntry (k, v) [] = [(k, v)]
insertEntry (k, v) ((k', v') : rest)
  | k == k' = (k, v) : rest -- replace if same key
  | k < k' = (k, v) : (k', v') : rest
  | otherwise = (k', v') : insertEntry (k, v) rest

-- replace the pair of children at idx with two children
replaceChildPair :: [BTreeNode k v] -> Int -> BTreeNode k v -> BTreeNode k v -> [BTreeNode k v]
replaceChildPair cs idx left right = take idx cs ++ [left, right] ++ drop (idx + 1) cs

replaceSingleChild :: [BTreeNode k v] -> Int -> BTreeNode k v -> [BTreeNode k v]
replaceSingleChild cs idx newChild = take idx cs ++ [newChild] ++ drop (idx + 1) cs

-- TEST HELPERS
emptyBTree :: Int -> BTree Int String
emptyBTree minDegree = BTree minDegree (BTreeNode [] [])

insertMany :: (Ord k) => Int -> [(k, v)] -> BTree k v -> BTree k v
insertMany _ [] bt = bt
insertMany t ((k, v) : rest) bt = insertMany t rest (insertBTree k v bt)

deleteMany :: (Ord k, Eq v) => [k] -> BTree k v -> BTree k v
deleteMany ks bt = foldl (flip deleteBTree) bt ks

tVal :: Int
tVal = 2

-- UNIT TESTS
testEmptyTreeSearch :: Test
testEmptyTreeSearch = TestCase $ do
  let bt = emptyBTree tVal
  assertEqual "Search in empty tree should return Nothing" Nothing (searchBTree bt 10)

testInsertSingleElement :: Test
testInsertSingleElement = TestCase $ do
  let bt = insertBTree 10 "ten" (emptyBTree tVal)
  assertEqual "Insert single elt and search for it" (Just "ten") (searchBTree bt 10)

testInsertTwoElements :: Test
testInsertTwoElements = TestCase $ do
  let bt = insertBTree 10 "ten" (emptyBTree tVal)
      bt2 = insertBTree 20 "twenty" bt
  assertEqual "Insert two elts and search for second one" (Just "twenty") (searchBTree bt2 20)

testInsertMultipleElements :: Test
testInsertMultipleElements = TestCase $ do
  let keys = [0 .. 5]
      vals = map show keys
      bt = insertMany tVal (zip keys vals) (emptyBTree tVal)
  mapM_ (\k -> assertEqual ("Inserted key: " ++ show k) (Just (show k)) (searchBTree bt k)) keys

testSearchNonExistingKey :: Test
testSearchNonExistingKey = TestCase $ do
  let keys = [0 .. 5]
      vals = map show keys
      bt = insertMany tVal (zip keys vals) (emptyBTree tVal)
  assertEqual "Search for a non-existing key should return Nothing" Nothing (searchBTree bt 100)

testDeleteExistingKey :: Test
testDeleteExistingKey = TestCase $ do
  let keys = [0 .. 5]
      vals = map show keys
      bt = insertMany tVal (zip keys vals) (emptyBTree tVal)
      btDel = deleteBTree 3 bt
  assertEqual "Delete an existing key and ensure its removed" Nothing (searchBTree btDel 3)

testDeleteNonExistingKey :: Test
testDeleteNonExistingKey = TestCase $ do
  let keys = [0 .. 5]
      vals = map show keys
      bt = insertMany tVal (zip keys vals) (emptyBTree tVal)
      btDel = deleteBTree 999 bt
  -- Deleting non-existing key should not affect other keys
  assertEqual "Deleting a non-existing key should not affect existing keys" (Just "0") (searchBTree btDel 0)

testSplitRootOnInsert :: Test
testSplitRootOnInsert = TestCase $ do
  let keys = [1, 2, 3]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
      bt2 = insertBTree 4 "4" bt
  mapM_ (\k -> assertEqual ("After splitting root, check inserted key: " ++ show k) (Just (show k)) (searchBTree bt2 k)) (keys ++ [4])

testInsertReversedOrder :: Test
testInsertReversedOrder = TestCase $ do
  let keys = [50, 40, 30, 20, 10]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
  mapM_ (\k -> assertEqual ("Inserted key: " ++ show k) (Just (show k)) (searchBTree bt k)) keys

testDeleteFromMultiSplitTree :: Test
testDeleteFromMultiSplitTree = TestCase $ do
  let keys = [10, 20, 5, 6, 12, 30, 7, 17]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
      btDel = deleteBTree 6 bt
  assertEqual "Delete key 6 and ensure its removed" Nothing (searchBTree btDel 6)
  assertEqual "Ensure other keys remain after deletion" (Just "7") (searchBTree btDel 7)

testRootBecomesEmptyAfterDeletion :: Test
testRootBecomesEmptyAfterDeletion = TestCase $ do
  let bt = insertBTree 42 "fortytwo" (emptyBTree tVal)
      btDel = deleteBTree 42 bt
  assertEqual "After deleting the only key, tree should be empty" Nothing (searchBTree btDel 42)

testUpdateDuplicateKey :: Test
testUpdateDuplicateKey = TestCase $ do
  let bt = insertBTree 10 "first" (emptyBTree tVal)
      bt2 = insertBTree 10 "second" bt
  assertEqual "Inserting a duplicate key should update its value" (Just "second") (searchBTree bt2 10)

testInsertAscendingOrder :: Test
testInsertAscendingOrder = TestCase $ do
  let keys = [1 .. 20]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
  mapM_ (\k -> assertEqual ("Check ascending key " ++ show k) (Just (show k)) (searchBTree bt k)) keys

testInsertDescendingOrder :: Test
testInsertDescendingOrder = TestCase $ do
  let keys = [20, 19 .. 1]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
  mapM_ (\k -> assertEqual ("Check descending key " ++ show k) (Just (show k)) (searchBTree bt k)) [1 .. 20]

testManualInsertDeleteScenario :: Test
testManualInsertDeleteScenario = TestCase $ do
  let bt = emptyBTree tVal
      bt1 = insertBTree 5 "five" bt
      bt2 = insertBTree 3 "three" bt1
      bt3 = insertBTree 4 "four" bt2
      bt4 = deleteBTree 3 bt3
  assertEqual "After deleting key 3, it should be removed" Nothing (searchBTree bt4 3)
  assertEqual "Key 4 should still be present" (Just "four") (searchBTree bt4 4)

testMergeAfterDeletion :: Test
testMergeAfterDeletion = TestCase $ do
  let keys = [2, 4, 6, 8, 10, 1, 3, 5, 7, 9]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
      btDel = deleteBTree 5 bt
  assertEqual "After deleting key 5, it should be removed" Nothing (searchBTree btDel 5)
  assertEqual "Key 6 should still be present" (Just "6") (searchBTree btDel 6)

testReinsertDeletedKey :: Test
testReinsertDeletedKey = TestCase $ do
  let bt = emptyBTree tVal
      bt1 = insertMany tVal (map (\x -> (x, show x)) [10, 20, 5, 15]) bt
      bt2 = deleteBTree 10 bt1
      bt3 = insertBTree 10 "Arman" bt2
  assertEqual "Reinserting deleted key 10 should return its new value" (Just "Arman") (searchBTree bt3 10)
  assertEqual "20 should still be present" (Just "20") (searchBTree bt3 20)

testInsertDuplicateKeys :: Test
testInsertDuplicateKeys = TestCase $ do
  let bt = emptyBTree tVal
      bt1 = insertBTree 5 "five" bt
      bt2 = insertBTree 5 "FIVE" bt1
      bt3 = insertBTree 6 "six" bt2
      bt4 = insertBTree 5 "Vincent" bt3
  assertEqual "After multiple dup inserts, key 5 should have the latest value" (Just "Vincent") (searchBTree bt4 5)
  assertEqual "Key 6 should still be present" (Just "six") (searchBTree bt4 6)

testDeleteWithNegativeKeys :: Test
testDeleteWithNegativeKeys = TestCase $ do
  let keys = [0, 1, 2, -2, 3, 4, 5, -3, -4, -1]
      bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
      btDel = deleteMany [2, 4, 6, 8, 10] bt
  mapM_
    ( \k ->
        if k `elem` [2, 4, 6, 8, 10]
          then assertEqual ("Deleted key " ++ show k ++ " should return Nothing") Nothing (searchBTree btDel k)
          else assertEqual ("Remaining key " ++ show k ++ " should still be present") (Just (show k)) (searchBTree btDel k)
    )
    keys

unitTests :: Test
unitTests =
  TestList
    [ testEmptyTreeSearch,
      testInsertSingleElement,
      testInsertTwoElements,
      testInsertMultipleElements,
      testSearchNonExistingKey,
      testDeleteExistingKey,
      testDeleteNonExistingKey,
      testSplitRootOnInsert,
      testInsertReversedOrder,
      testDeleteFromMultiSplitTree,
      testRootBecomesEmptyAfterDeletion,
      testUpdateDuplicateKey,
      testInsertAscendingOrder,
      testInsertDescendingOrder,
      testManualInsertDeleteScenario,
      testMergeAfterDeletion,
      testReinsertDeletedKey,
      testInsertDuplicateKeys,
      testDeleteWithNegativeKeys
    ]

-- QUICKCHECK PROPERTIES
prop_insertSearch :: [Int] -> Property
prop_insertSearch xs =
  not (null xs)
    ==> let keys = nub xs
            bt = insertMany tVal (map (\x -> (x, show x)) keys) (emptyBTree tVal)
         in all (\k -> searchBTree bt k == Just (show k)) keys

prop_insertOrdered :: [Int] -> Bool
prop_insertOrdered xs =
  let sortedKeys = sort (nub xs)
      bt = insertMany tVal (map (\x -> (x, show x)) sortedKeys) (emptyBTree tVal)
   in all (\k -> searchBTree bt k == Just (show k)) sortedKeys

prop_insertReverse :: [Int] -> Bool
prop_insertReverse xs =
  let sortedKeys = sort (nub xs)
      revKeys = reverse sortedKeys
      bt = insertMany tVal (map (\x -> (x, show x)) revKeys) (emptyBTree tVal)
   in all (\k -> searchBTree bt k == Just (show k)) sortedKeys

runAllTests :: IO ()
runAllTests = do
  putStrLn "Running unit tests"
  _ <- runTestTT unitTests
  putStrLn "\nRunning QC properties"
  QC.quickCheck prop_insertSearch
  QC.quickCheck prop_insertOrdered
  QC.quickCheck prop_insertReverse