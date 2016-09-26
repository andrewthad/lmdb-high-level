{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module Main (main) where

import Lmdb.Types
import Lmdb.Function
import qualified Lmdb.Function as Lmdb
import qualified Lmdb.Codec as Codec
import qualified Lmdb.Cursor.Get as Cursor
import qualified Data.List as List
import Data.Foldable
import Data.Word
import System.Random
import Foreign.Marshal.Alloc
import System.Directory
import Test.HUnit                           (Assertion,(@?=),assertBool,assertFailure)
import Test.Framework                       (testGroup, Test, defaultMain)
import Test.QuickCheck (Gen, Arbitrary(..), choose, Property)
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
  , testProperty "Put Get Law (Text)" textCodecTest
  ]

testDbiSettings :: DbiSettings Word Word
testDbiSettings = makeSettings
  SortIntegralIncreasing Codec.word Codec.word

textualDbiSettings :: DbiSettings Text Text
textualDbiSettings = makeSettings
  SortLexographic Codec.text Codec.text

wordTextDbiSettings :: DbiSettings Word Text
wordTextDbiSettings = makeSettings
  SortIntegralIncreasing Codec.word Codec.text

putGetLaw :: Word -> Word -> Property
putGetLaw k v = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb testDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Lmdb.insert txn db k v
        m <- Lmdb.lookup (readonly txn) db k
        return (m == Just v)

putGetLawSeparate :: Word -> Word -> Property
putGetLawSeparate k v = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb testDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Lmdb.insert txn db k v
      m <- withTransaction env $ \txn -> do
        Lmdb.lookup (readonly txn) db k
      return (m == Just v)

wordOrdering :: [Word] -> Property
wordOrdering ks = monadicIO $ do
  (assert =<<) $ run $ do
    withOneDb wordTextDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        forM_ ks $ \k -> Lmdb.insert txn db k Text.empty
        withCursor txn db $ \cur -> do
          xs <- Pipes.toListM (Cursor.firstForward cur)
          return (map keyValueKey xs == List.sort ks)

textCodecTest :: Text -> Text -> Property
textCodecTest k v = monadicIO $ do
  pre (not $ Text.null k)
  pre (Text.length k < 512)
  (assertWith =<<) $ run $ do
    withOneDb textualDbiSettings $ \env db -> do
      withTransaction env $ \txn -> do
        Lmdb.insert txn db k v
        m <- Lmdb.lookup (readonly txn) db k
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

propCodecIso :: (Eq a,Show a) => Codec a -> a -> Property
propCodecIso (Codec encode decode) original = do
  let Encoding sz f = encode original
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
     DbiSettings k v
  -> (Environment 'ReadWrite -> DbiHandle k v -> IO a)
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

instance Arbitrary Text where
  arbitrary = fmap Text.pack arbitrary
  shrink = map Text.pack . shrink . Text.unpack


