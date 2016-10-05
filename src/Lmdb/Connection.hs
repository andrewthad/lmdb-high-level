{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE BangPatterns #-}

module Lmdb.Connection where

import Database.LMDB.Raw
import Lmdb.Types
import Lmdb.Internal
import Data.Word
import Foreign.Storable
import Data.Coerce
import Data.Functor
import Data.Bits
import Control.Concurrent (runInBoundThread,isCurrentThreadBound)
import Data.Bool (bool)
import System.IO (withFile,IOMode(ReadMode))
import Foreign.C.Types (CSize(..))
import Foreign.Ptr (Ptr,plusPtr)
import Foreign.Marshal.Alloc (allocaBytes,alloca)
import Control.Monad
import Control.Exception (finally, bracketOnError)

withCursor ::
     Transaction e
  -> Database k v
  -> (Cursor e k v -> IO a)
  -> IO a
withCursor (Transaction txn) (Database dbi settings) f = do
  cur <- mdb_cursor_open_X txn dbi
  a <- f (Cursor cur settings)
  mdb_cursor_close_X cur
  return a

withMultiCursor ::
     Transaction e
  -> MultiDatabase k v
  -> (MultiCursor e k v -> IO a)
  -> IO a
withMultiCursor (Transaction txn) (MultiDatabase dbi settings) f = do
  cur <- mdb_cursor_open_X txn dbi
  a <- f (MultiCursor cur settings)
  mdb_cursor_close_X cur
  return a

withAbortableTransaction ::
     ModeBool e
  => Environment e
  -> (Transaction e -> IO (Maybe a))
  -> IO (Maybe a)
withAbortableTransaction e@(Environment env) f = do
  let isReadOnly = modeIsReadOnly e
  txn <- mdb_txn_begin env Nothing isReadOnly
  ma <- f (Transaction txn)
  case ma of
    Nothing -> do
      mdb_txn_abort txn
      return Nothing
    Just a -> do
      mdb_txn_commit txn
      return (Just a)

withTransaction ::
     ModeBool e
  => Environment e
  -> (Transaction e -> IO a)
  -> IO a
withTransaction e@(Environment env) f = do
  let isReadOnly = modeIsReadOnly e
  bool runInBoundThread id isReadOnly $ bracketOnError
    (mdb_txn_begin env Nothing isReadOnly)
    mdb_txn_abort
    $ \txn -> do
      a <- f (Transaction txn)
      mdb_txn_commit txn
      return a

withNestedTransaction ::
     Environment 'ReadWrite
  -> Transaction 'ReadWrite
  -> (Transaction 'ReadWrite -> IO a)
  -> IO a
withNestedTransaction e@(Environment env) (Transaction parentTxn) f = do
  txn <- mdb_txn_begin env (Just parentTxn) True
  a <- f (Transaction txn)
  mdb_txn_commit txn
  return a

-- This function can be improved to handle custom sorting.
openDatabase ::
     ModeBool e
  => Transaction e
  -> Maybe String -- ^ Database name
  -> DatabaseSettings k v
  -> IO (Database k v)
openDatabase t@(Transaction txn) name settings = do
  let rwOpts = if modeIsReadOnly t then [] else [MDB_CREATE]
      (keySafeFfi, keyExtraCmd, sortOpts) = case settings of
        DatabaseSettings keySort _ keyDec _ _ -> case keySort of
          SortNative s -> (,,) False (\_ -> return ()) $ case s of
            NativeSortLexographic -> []
            NativeSortLexographicBackward -> [MDB_REVERSEKEY]
            NativeSortInteger -> [MDB_INTEGERKEY]
          SortCustom s -> customSortConfig True keyDec t s
      opts = rwOpts ++ sortOpts
  dbi <- mdb_dbi_open_X keySafeFfi txn name opts
  keyExtraCmd dbi
  return (Database dbi settings)

customSortConfig :: Bool -> Decoding a -> Transaction e -> CustomSort a -> (Bool, DbiByFfi -> IO (), [MDB_DbFlag])
customSortConfig isKey (Decoding decode) (Transaction txn) s = (,,) True (\dbiOuter -> case dbiOuter of
  DbiSafe dbi -> case s of
    CustomSortUnsafe f -> setCmp txn dbi f
    CustomSortSafe f -> (setCmp txn dbi =<<) $ wrapCmpFn $ \aPtr bPtr -> do
      MDB_val aSize aData <- peek aPtr
      MDB_val bSize bData <- peek bPtr
      a <- decode aSize aData
      b <- decode bSize bData
      return $ case f a b of
        GT -> 1
        EQ -> 0
        LT -> (-1)
  DbiUnsafe _ -> fail "customSortConfig: logical error in sorting. Open an issue if this happens."
  ) []
  where setCmp = if isKey then mdb_set_compare else mdb_set_dupsort

-- This function can be improved to handle custom sorting.
openMultiDatabase ::
     ModeBool e
  => Transaction e
  -> Maybe String -- ^ Database name
  -> MultiDatabaseSettings k v
  -> IO (MultiDatabase k v)
openMultiDatabase t@(Transaction txn) name settings = do
  let rwOpts = if modeIsReadOnly t then [] else [MDB_CREATE]
      (keySafeFfi,keyExtraCmd,keySortOpts) = case settings of
        MultiDatabaseSettings keySort _ _ keyDec _ _ -> case keySort of
          SortNative s -> (,,) False (\_ -> return ()) $ case s of
            NativeSortLexographic -> []
            NativeSortLexographicBackward -> [MDB_REVERSEKEY]
            NativeSortInteger -> [MDB_INTEGERKEY]
          SortCustom s -> customSortConfig True keyDec t s
      (valSafeFfi,valExtraCmd,valSortOpts) = case settings of
        MultiDatabaseSettings _ valSort _ _ _ valDec -> case valSort of
          SortNative s -> (,,) False (\_ -> return ()) $ case s of
            NativeSortLexographic -> []
            NativeSortLexographicBackward -> [MDB_REVERSEDUP]
            NativeSortInteger -> [MDB_INTEGERDUP]
          SortCustom s -> customSortConfig False valDec t s
      multiOpts = case settings of
        MultiDatabaseSettings _ _ _ _ valEnc _ -> case valEnc of
          EncodingVariable _ -> []
          EncodingMachineWord _ -> [MDB_DUPFIXED]
          EncodingFixed _ _ -> [MDB_DUPFIXED]
      baseOpts = [MDB_DUPSORT]
      safeFfi = keySafeFfi || valSafeFfi
      opts = rwOpts ++ keySortOpts ++ valSortOpts ++ multiOpts ++ baseOpts
  dbi <- mdb_dbi_open_X safeFfi txn name opts
  keyExtraCmd dbi
  valExtraCmd dbi
  return (MultiDatabase dbi settings)

-- | This should not normally be used.
withDatabase ::
     ModeBool e
  => Environment e
  -> Transaction e
  -> Maybe String
  -> DatabaseSettings k v
  -> (Database k v -> IO a)
  -> IO a
withDatabase env txn mname settings f = do
  dbi <- openDatabase txn mname settings
  finally (f dbi) (closeDatabase env dbi)

-- | This should not normally be used.
withMultiDatabase ::
     ModeBool e
  => Environment e
  -> Transaction e
  -> Maybe String
  -> MultiDatabaseSettings k v
  -> (MultiDatabase k v -> IO a)
  -> IO a
withMultiDatabase env txn mname settings f = do
  dbi <- openMultiDatabase txn mname settings
  finally (f dbi) (closeMultiDatabase env dbi)

-- | Internally, this calls @mdb_env_create@ and @mdb_env_open@.
initializeReadOnlyEnvironment ::
     Int -- ^ Map size in bytes
  -> Int -- ^ Maximum number of readers (recommended: 126)
  -> Int -- ^ Maximum number of databases
  -> FilePath -- ^ Directory for lmdb data and locks
  -> IO (Environment 'ReadOnly)
initializeReadOnlyEnvironment =
  initializeEnvironmentInternal [MDB_RDONLY]

withReadOnlyEnvironment ::
     Int -- ^ Map size in bytes
  -> Int -- ^ Maximum number of readers (recommended: 126)
  -> Int -- ^ Maximum number of databases
  -> FilePath -- ^ Directory for lmdb data and locks
  -> (Environment 'ReadOnly -> IO a) -- ^ Computation requiring an 'Environment'
  -> IO a
withReadOnlyEnvironment a b c d f = do
  env <- initializeReadOnlyEnvironment a b c d
  finally (f env) (closeEnvironment env)

-- | Internally, this calls @mdb_env_create@ and @mdb_env_open@.
initializeReadWriteEnvironment ::
     Int -- ^ Map size in bytes
  -> Int -- ^ Maximum number of readers (recommended: 126)
  -> Int -- ^ Maximum number of databases
  -> FilePath -- ^ Directory for lmdb data and locks
  -> IO (Environment 'ReadWrite)
initializeReadWriteEnvironment = initializeEnvironmentInternal []

-- | It is not clear whether or not it is actually neccessary
--   to use 'runInBoundThread' here. It is done just as an extra
--   precaution.
initializeEnvironmentInternal ::
     [MDB_EnvFlag] -- ^ Flags
  -> Int -- ^ Map size in bytes
  -> Int -- ^ Maximum number of readers (recommended: 126)
  -> Int -- ^ Maximum number of databases
  -> FilePath -- ^ Directory for lmdb data and locks
  -> IO (Environment e)
initializeEnvironmentInternal flags maxSize maxReaders maxDbs dir =
  runInBoundThread $ do
    env <- mdb_env_create
    mdb_env_set_mapsize env maxSize
    mdb_env_set_maxreaders env maxReaders
    mdb_env_set_maxdbs env maxDbs
    mdb_env_open env dir flags
    return (Environment env)

closeDatabase :: Environment e -> Database k v -> IO ()
closeDatabase (Environment env) (Database dbi _) =
  mdb_dbi_close_X env dbi

closeMultiDatabase :: Environment e -> MultiDatabase k v -> IO ()
closeMultiDatabase (Environment env) (MultiDatabase dbi _) =
  mdb_dbi_close_X env dbi

closeEnvironment :: Environment e -> IO ()
closeEnvironment (Environment env) =
  runInBoundThread $ mdb_env_close env

makeSettings ::
     Sort s k -- ^ Key sorting function
  -> Codec s k -- ^ Key codec
  -> Codec sv v -- ^ Value codec
  -> DatabaseSettings k v
makeSettings sort (Codec kEnc kDec) (Codec vEnc vDec) =
  DatabaseSettings sort kEnc kDec vEnc vDec

makeMultiSettings ::
     Sort sk k -- ^ Key sorting function
  -> Sort sv v -- ^ Value sorting function
  -> Codec sk k -- ^ Key codec
  -> Codec sv v -- ^ Value codec
  -> MultiDatabaseSettings k v
makeMultiSettings ksort vsort (Codec kEnc kDec) (Codec vEnc vDec) =
  MultiDatabaseSettings ksort vsort kEnc kDec vEnc vDec

readonly :: Transaction 'ReadWrite -> Transaction 'ReadOnly
readonly = coerce

readonlyEnvironment :: Environment 'ReadWrite -> Environment 'ReadOnly
readonlyEnvironment = coerce

-- csvsToLmdb ::
--      FilePath -- ^ Geolite IPv4 Blocks
--   -> FilePath -- ^ Geolite City Locations
--   -> FilePath -- ^ Directory for LMDB
--   -> IO ()
-- csvsToLmdb geoBlocksPath geoCitiesPath lmdbDir = do
--   env <- mdb_env_create
--   mdb_env_set_mapsize env (2 ^ 31)
--   mdb_env_set_maxreaders env 1
--   mdb_env_set_maxdbs env 2
--   mdb_env_open env lmdbDir []
--   txn <- mdb_txn_begin env Nothing True
--   dbiBlocks <- mdb_dbi_open' txn (Just "blocks") [MDB_INTEGERKEY,MDB_CREATE]
--   let appendFlag = compileWriteFlags [MDB_APPEND]
--   r <- withFile filename ReadMode $ \h -> runEffect $
--         fmap (SD.convertDecodeError "utf-8") (PT.decode (PT.utf8 . PT.eof) $ PB.fromHandle h)
--     >-> fmap Just blocks
--     >-> Pipes.mapM_ (\block -> do
--           alloca $ \(w64Ptr :: Ptr Word64) -> do
--             poke w64Ptr (fromIntegral w32)
--             let IPv4Range (IPv4 w32) _ = blockNetwork block
--                 w8Ptr = (castPtr :: Ptr Word64 -> Ptr Word8) w64Ptr
--             mdb_put' appendFlag txn dbiBlocks
--               (MDB_val (CSize 8) w8Ptr)
--               (MDB_val (CSize 8) w8Ptr)
--         )
--   case r of
--     Nothing -> assertBool "impossible" True
--     Just err -> assertFailure (Decoding.prettyError Text.unpack err)

-- putBlock :: Block -> Put
-- putBlock (Block network geonameId registered represented isAnon isSat postal lat lon accuracy)


