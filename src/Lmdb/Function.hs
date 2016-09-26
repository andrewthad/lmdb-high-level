{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE BangPatterns #-}

module Lmdb.Function where

import Database.LMDB.Raw
import Lmdb.Types
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

withCursor ::
     Transaction e
  -> DbiHandle k v
  -> (Cursor e k v -> IO a)
  -> IO a
withCursor (Transaction txn) (DbiHandle dbi settings) f = do
  cur <- mdb_cursor_open' txn dbi
  a <- f (Cursor cur settings)
  mdb_cursor_close' cur
  return a

withAbortableTransaction ::
     ModeBool e
  => Environment e
  -> (Transaction e -> IO (Maybe a))
  -> IO (Maybe a)
withAbortableTransaction e@(Environment env) f = do
  txn <- mdb_txn_begin env Nothing (modeIsReadOnly e)
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
  bool runInBoundThread id isReadOnly $ do
    txn <- mdb_txn_begin env Nothing isReadOnly
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
  -> Maybe String
  -> DbiSettings k v
  -> IO (DbiHandle k v)
openDatabase t@(Transaction txn) name settings = do
  let rwOpts = if modeIsReadOnly t then [] else [MDB_CREATE]
  dbi <- mdb_dbi_open' txn name rwOpts
  return (DbiHandle dbi settings)

-- | Internally, this calls @mdb_env_create@ and @mdb_env_open@.
initializeReadOnlyEnvironment ::
     Int -- ^ Map size in bytes
  -> Int -- ^ Maximum number of readers (recommended: 126)
  -> Int -- ^ Maximum number of databases
  -> FilePath -- ^ Directory for lmdb data and locks
  -> IO (Environment 'ReadOnly)
initializeReadOnlyEnvironment =
  initializeEnvironmentInternal [MDB_RDONLY]

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

closeDatabase :: Environment e -> DbiHandle k v -> IO ()
closeDatabase (Environment env) (DbiHandle dbi _) =
  mdb_dbi_close' env dbi

closeEnvironment :: Environment e -> IO ()
closeEnvironment (Environment env) =
  runInBoundThread $ mdb_env_close env

makeSettings ::
     Sort k -- ^ Key sorting function
  -> Codec k -- ^ Key codec
  -> Codec v -- ^ Value codec
  -> DbiSettings k v
makeSettings sort (Codec kEnc kDec) (Codec vEnc vDec) =
  DbiSettings sort kEnc kDec vEnc vDec

--
-- openEnvironmentReadOnly ::
--   En

lookup :: Transaction 'ReadOnly -> DbiHandle k v -> k -> IO (Maybe v)
lookup (Transaction txn) (DbiHandle dbi settings) k = do
  let encodeKey = dbiSettingsEncodeKey settings
      decodeValue = dbiSettingsDecodeValue settings
      Encoding (CSize keySize) keyPoke = encodeKey k
  m <- allocaBytes (fromIntegral keySize) $ \keyPtr -> do
    keyPoke keyPtr
    mdb_get' txn dbi (MDB_val (CSize $ fromIntegral keySize) keyPtr)
  case m of
    Nothing -> return Nothing
    Just (MDB_val valSize valPtr) -> fmap Just $ decodeValue valSize valPtr

insertInternal :: MDB_WriteFlags -> Transaction 'ReadWrite -> DbiHandle k v -> k -> v -> IO Bool
insertInternal flags (Transaction txn) (DbiHandle dbi settings) k v = do
  let encodeKey = dbiSettingsEncodeKey settings
      encodeVal = dbiSettingsEncodeValue settings
      Encoding (CSize keySize) keyPoke = encodeKey k
      Encoding (CSize valSize) valPoke = encodeVal v
  allocaBytes (fromIntegral keySize) $ \keyPtr -> do
    allocaBytes (fromIntegral valSize) $ \valPtr -> do
      keyPoke keyPtr
      valPoke valPtr
      mdb_put' flags txn dbi
        (MDB_val (CSize $ fromIntegral keySize) keyPtr)
        (MDB_val (CSize $ fromIntegral valSize) valPtr)

insertInternal' :: MDB_WriteFlags -> Transaction 'ReadWrite -> DbiHandle k v -> k -> v -> IO ()
insertInternal' a b c d e = insertInternal a b c d e $> ()

insert :: Transaction 'ReadWrite -> DbiHandle k v -> k -> v -> IO ()
insert = insertInternal' noWriteFlags

insertNoOverride :: Transaction 'ReadWrite -> DbiHandle k v -> k -> v -> IO Bool
insertNoOverride = insertInternal noOverwriteFlags

append :: Transaction 'ReadWrite -> DbiHandle k v -> k -> v -> IO ()
append = insertInternal' appendFlags

readonly :: Transaction 'ReadWrite -> Transaction 'ReadOnly
readonly = coerce

noWriteFlags :: MDB_WriteFlags
noWriteFlags = compileWriteFlags []

noOverwriteFlags :: MDB_WriteFlags
noOverwriteFlags = compileWriteFlags [MDB_NOOVERWRITE]

appendFlags :: MDB_WriteFlags
appendFlags = compileWriteFlags [MDB_APPEND]

mdb_val_size :: Int
mdb_val_size = sizeOf (undefined :: MDB_val)

-- | Alternative to 'withKVPtrs' that allows us to not initialize the key or the
--   value.
withKVPtrsNoInit :: (Ptr MDB_val -> Ptr MDB_val -> IO a) -> IO a
withKVPtrsNoInit fn =
  allocaBytes (unsafeShiftL mdb_val_size 1) $ \pK ->
    let pV = pK `plusPtr` mdb_val_size
     in fn pK pV
{-# INLINE withKVPtrsNoInit #-}

withKVPtrsInitKey :: MDB_val -> (Ptr MDB_val -> Ptr MDB_val -> IO a) -> IO a
withKVPtrsInitKey k fn =
  allocaBytes (unsafeShiftL mdb_val_size 1) $ \pK ->
    let pV = pK `plusPtr` mdb_val_size
     in poke pK k >> fn pK pV
{-# INLINE withKVPtrsInitKey #-}

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

