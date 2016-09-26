{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}

module Lmdb.Cursor.Get
  ( first
  , last
  , next
  , prev
  , at
  , at'
  , firstForward
  ) where

import Prelude hiding (last)
import Foreign.Ptr (Ptr)
import Lmdb.Function
import Lmdb.Types
import Foreign.Storable
import Database.LMDB.Raw
import Data.Word
import Control.Monad.Trans.Class
import Pipes (yield, Producer')
import Pipes.Core (respond,Server')
import Foreign.Marshal.Alloc (allocaBytes,alloca)

first :: Cursor e k v -> IO (Maybe (KeyValue k v))
first = getWithoutKey MDB_FIRST

last :: Cursor e k v -> IO (Maybe (KeyValue k v))
last = getWithoutKey MDB_LAST

next :: Cursor e k v -> IO (Maybe (KeyValue k v))
next = getWithoutKey MDB_NEXT

prev :: Cursor e k v -> IO (Maybe (KeyValue k v))
prev = getWithoutKey MDB_PREV

-- | Uses @MDB_SET_KEY@
at :: Cursor e k v -> k -> IO (Maybe (KeyValue k v))
at = getWithKey MDB_SET_KEY

-- | Uses @MDB_SET_RANGE@
at' :: Cursor e k v -> k -> IO (Maybe (KeyValue k v))
at' = getWithKey MDB_SET_RANGE

yieldMaybeThen ::
     (Cursor e k v -> IO (Maybe (KeyValue k v)))
  -> (Cursor e k v -> Producer' (KeyValue k v) IO ())
  -> Cursor e k v
  -> Producer' (KeyValue k v) IO ()
yieldMaybeThen f p cursor = do
  m <- lift (f cursor)
  case m of
    Nothing -> return ()
    Just kv -> yield kv >> p cursor

repeatedly :: forall e k v. (Cursor e k v -> IO (Maybe (KeyValue k v))) -> Cursor e k v -> Producer' (KeyValue k v) IO ()
repeatedly f = go
  where
  go :: Cursor e k v -> Producer' (KeyValue k v) IO ()
  go = yieldMaybeThen f go

forward :: Cursor e k v -> Producer' (KeyValue k v) IO ()
forward = repeatedly next

backward :: Cursor e k v -> Producer' (KeyValue k v) IO ()
backward = repeatedly prev

firstForward :: Cursor e k v -> Producer' (KeyValue k v) IO ()
firstForward = yieldMaybeThen first forward

lastBackward :: Cursor e k v -> Producer' (KeyValue k v) IO ()
lastBackward = yieldMaybeThen last backward

atForward :: Cursor e k v -> k -> Producer' (KeyValue k v) IO ()
atForward cursor k = yieldMaybeThen (flip at k) forward cursor

atForward' :: Cursor e k v -> k -> Producer' (KeyValue k v) IO ()
atForward' cursor k = yieldMaybeThen (flip at' k) forward cursor

server :: forall e k v.
     Cursor e k v
  -> (Cursor e k v -> IO (Maybe (KeyValue k v)))
  -> Server' (Cursor e k v -> IO (Maybe (KeyValue k v))) (KeyValue k v) IO ()
server cursor initialAction = lift (initialAction cursor) >>= go
  where
  go :: Maybe (KeyValue k v)
     -> Server' (Cursor e k v -> IO (Maybe (KeyValue k v))) (KeyValue k v) IO ()
  go Nothing = return ()
  go (Just kv) = do
    action <- respond kv
    m <- lift (action cursor)
    go m

getWithKey :: MDB_cursor_op -> Cursor e k v -> k -> IO (Maybe (KeyValue k v))
getWithKey op (Cursor cur settings) k = do
  let Encoding keySize keyPoke = dbiSettingsEncodeKey settings k
  allocaBytes (fromIntegral keySize) $ \(keyDataPtr :: Ptr Word8) -> do
    keyPoke keyDataPtr
    withKVPtrsInitKey (MDB_val keySize keyDataPtr) $ \keyPtr valPtr -> do
      success <- mdb_cursor_get' MDB_FIRST cur keyPtr valPtr
      decodeResults success settings keyPtr valPtr

getWithoutKey :: MDB_cursor_op -> Cursor e k v -> IO (Maybe (KeyValue k v))
getWithoutKey op (Cursor cur settings) = do
  withKVPtrsNoInit $ \(keyPtr :: Ptr MDB_val) (valPtr :: Ptr MDB_val) -> do
    success <- mdb_cursor_get' MDB_FIRST cur keyPtr valPtr
    decodeResults success settings keyPtr valPtr

decodeResults :: Bool -> DbiSettings k v -> Ptr MDB_val -> Ptr MDB_val -> IO (Maybe (KeyValue k v))
decodeResults success settings keyPtr valPtr = if success
  then do
    MDB_val keySize keyWordPtr <- peek keyPtr
    MDB_val valSize valWordPtr <- peek valPtr
    key <- dbiSettingsDecodeKey settings keySize keyWordPtr
    val <- dbiSettingsDecodeValue settings valSize valWordPtr
    return (Just (KeyValue key val))
  else return Nothing
{-# INLINE decodeResults #-}

