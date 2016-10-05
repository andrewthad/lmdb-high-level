{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module Main (main) where

import Lmdb.Types
import Lmdb.Connection
import Lmdb.Internal (runEncoding)
import Data.Monoid (All(..))
import Control.Monad
import qualified Lmdb.Connection as Lmdb
import qualified Lmdb.Codec as Codec
import qualified Lmdb.Map as Map
import qualified Lmdb.Multimap as Multimap
import qualified Data.List as List
import qualified Data.ByteString.Char8 as BC8
import qualified Data.Set as Set
import qualified Data.Map as CMap
import Text.Read (readMaybe)
import Data.ByteString (ByteString)
import Data.Foldable
import Data.Word
import System.Random
import Foreign.Marshal.Alloc
import System.Directory
import Test.HUnit                           (Assertion,(@?=),assertBool,assertFailure)
import Test.Framework                       (testGroup, Test, defaultMain)
import Test.QuickCheck (Gen, Arbitrary(..), choose, Property, arbitraryBoundedEnum)
import Test.QuickCheck.Monadic (monadicIO, assert, run, pre, PropertyM)

import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.Framework.Providers.HUnit       (testCase)
import Debug.Trace
import Pipes
import Control.Concurrent
import Control.Concurrent.MVar
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Pipes.Prelude as Pipes

main :: IO ()
main = do
  let dir = "data/test/"
  createDirectoryIfMissing True dir
  defaultMain tests
  -- For some reason, this does not end up deleting the
  -- directory. Hmm...
  -- removeDirectoryRecursive dir

tests :: [Test]
tests =
  [ testProperty "Put Get Law (single transaction)" putGetLaw
  , testProperty "Put Get Law (separate transactions)" putGetLawSeparate
  , testProperty "Text Codec" (propCodecIso Codec.text)
  , testProperty "ByteString Codec" (propCodecIso Codec.byteString)
  , testProperty "Put Get Law (Text)" textCodecTest
  , testProperty "Native Word Ordering" wordOrdering
  , testProperty "Custom Color Ordering" customColorOrdering
  , testProperty "Multimap Streaming Values at Key" multimapStreamingValues
  ]

testDbiSettings :: DatabaseSettings Word Word
testDbiSettings = makeSettings
  (SortNative NativeSortInteger) Codec.word Codec.word

textualDbiSettings :: DatabaseSettings Text Text
textualDbiSettings = makeSettings
  (SortNative NativeSortLexographic) Codec.text Codec.text

wordTextDbiSettings :: DatabaseSettings Word Text
wordTextDbiSettings = makeSettings
  (SortNative NativeSortInteger) Codec.word Codec.text

-- multiTestSettings :: MultiDatabaseSettings Word Word
-- multiTestSettings = makeMultiSettings
--   (SortNative NativeSortInteger)
--   (SortNative NativeSortInteger)
--   Codec.word
--   Codec.word

multiColorSettings :: MultiDatabaseSettings Word Color
multiColorSettings = makeMultiSettings
  (SortNative NativeSortInteger)
  (SortCustom $ CustomSortSafe compare)
  Codec.word
  (Codec.throughByteString colorToByteString colorFromByteString)

customSortColorSettings :: DatabaseSettings Color Word
customSortColorSettings = makeSettings
  (SortCustom $ CustomSortSafe compare)
  (Codec.throughByteString colorToByteString colorFromByteString)
  Codec.word

data Color = Red | Green | Blue | Purple | Orange | Yellow
  deriving (Show,Read,Eq,Ord,Enum,Bounded)

colorToByteString :: Color -> ByteString
colorToByteString = BC8.pack . show

colorFromByteString :: ByteString -> Maybe Color
colorFromByteString = readMaybe . BC8.unpack

putGetLaw :: Word -> Word -> Property
putGetLaw k v = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb testDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Map.insert' txn db k v
        m <- Map.lookup' (readonly txn) db k
        return (m == Just v)

putGetLawSeparate :: Word -> Word -> Property
putGetLawSeparate k v = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb testDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Map.insert' txn db k v
      m <- withTransaction env $ \txn -> do
        Map.lookup' (readonly txn) db k
      return (m == Just v)

wordOrdering :: [Word] -> Property
wordOrdering ks = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb wordTextDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        forM_ ks $ \k -> Map.insertSuccess' txn db k Text.empty
        let ksNoDups = Set.toList $ Set.fromList ks
        withCursor txn db $ \cur -> do
          xs <- Pipes.toListM (Map.firstForward cur)
          return (map keyValueKey xs == ksNoDups)

customColorOrdering :: [Color] -> Property
customColorOrdering ks = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb customSortColorSettings $ \env db -> do
      withTransaction env $ \txn -> do
        forM_ ks $ \k -> Map.insertSuccess' txn db k 55
        let ksNoDups = Set.toList $ Set.fromList ks
        withCursor txn db $ \cur -> do
          xs <- Pipes.toListM (Map.firstForward cur)
          return (map keyValueKey xs == ksNoDups)

multimapStreamingValues :: [(Word,Color)] -> Property
multimapStreamingValues xs = monadicIO $ do
  (assert =<<) $ run $ do
    withOneMultiDb multiColorSettings $ \env db -> do
      withTransaction env $ \txn -> do
        let xs2 = map (\(a,b) -> (a,Set.singleton b)) xs
            expected = CMap.toList $ fmap Set.toList $ CMap.fromListWith mappend xs2
        withMultiCursor txn db $ \cur -> do
          forM_ xs $ \(k,v) -> Multimap.insert cur k v
          All correct <- fmap mconcat $ forM expected $ \(k,expVals) -> do
            vals <- Pipes.toListM $ Multimap.lookupValues cur k
            return $ All $ vals == expVals
          return correct

textCodecTest :: Text -> Text -> Property
textCodecTest k v = monadicIO $ do
  pre (not $ Text.null k)
  pre (Text.length k < 512)
  (assertWith =<<) $ run $ do
    withOneDb textualDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Map.insert' txn db k v
      withTransaction env $ \txn -> do
        m <- Map.lookup' (readonly txn) db k
        return $ case m of
          Just found -> if found == v
            then Nothing
            else Just $ concat
              [ "Looking up key ["
              , Text.unpack k
              , "]: expected ["
              , Text.unpack v
              , "] but got ["
              , Text.unpack found
              , "]"
              ]
          Nothing -> Just $ concat
            [ "Tried to look up key ["
            , Text.unpack k
            , "] after insertion but no value was found"
            ]

propCodecIso :: (Eq a,Show a) => Codec s a -> a -> Property
propCodecIso (Codec encoding (Decoding decode)) original = do
  let SizedPoke sz f = runEncoding encoding original
  monadicIO $ (assertWith =<<) $ run $ allocaBytes (fromIntegral sz) $ \ptr -> do
    f ptr
    copied <- decode sz ptr
    return $ if copied == original
      then Nothing
      else Just $ concat
        [ "Original value: ["
        , show original
        , "] Decoded value: ["
        , show copied
        , "]"
        ]

assertWith :: Monad m => Maybe String -> PropertyM m ()
assertWith Nothing = return ()
assertWith (Just e) = fail e

withOneDb ::
     DatabaseSettings k v
  -> (Environment 'ReadWrite -> Database k v -> IO a)
  -> IO a
withOneDb s1 f = do
  dirNum <- randomIO :: IO Word64
  let dir = "data/test/" ++ show dirNum
  createDirectory dir
  env <- initializeReadWriteEnvironment 1000000 5 25 dir
  db1 <- withTransaction env $ \txn -> do
    openDatabase txn (Just "db1") s1
  a <- f env db1
  closeDatabase env db1
  closeEnvironment env
  removeDirectoryRecursive dir
  return a

withOneMultiDb ::
     MultiDatabaseSettings k v
  -> (Environment 'ReadWrite -> MultiDatabase k v -> IO a)
  -> IO a
withOneMultiDb s1 f = do
  dirNum <- randomIO :: IO Word64
  let dir = "data/test/" ++ show dirNum
  createDirectory dir
  env <- initializeReadWriteEnvironment 1000000 5 25 dir
  db1 <- withTransaction env $ \txn -> do
    openMultiDatabase txn (Just "db1") s1
  a <- f env db1
  closeMultiDatabase env db1
  closeEnvironment env
  removeDirectoryRecursive dir
  return a

instance Arbitrary Color where
  arbitrary = arbitraryBoundedEnum

instance Arbitrary Text where
  arbitrary = fmap Text.pack arbitrary
  shrink = map Text.pack . shrink . Text.unpack

instance Arbitrary ByteString where
  arbitrary = fmap BC8.pack arbitrary
  shrink = map BC8.pack . shrink . BC8.unpack

