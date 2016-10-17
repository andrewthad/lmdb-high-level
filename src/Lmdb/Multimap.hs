{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE BangPatterns #-}

module Lmdb.Multimap
  (
    lookupValues
  , insert
  , dupsert
  , delete
  , deleteValues
  , first
  ) where

import Prelude hiding (last,lookup)
import Foreign.Ptr (Ptr)
import Lmdb.Internal
import Lmdb.Types
import Foreign.Storable
import Database.LMDB.Raw
import Data.Word
import Control.Monad
import Control.Monad.Trans.Class
import Pipes (yield, Producer', Producer)
import Pipes.Core (respond,Server',(\>\),(/>/),(>+>),(>~>),request,pull,push)
import Foreign.Marshal.Alloc (allocaBytes,alloca)
import Foreign.C.Types (CSize(..))
import Control.Monad

-- Producer' (k,Producer' v IO ()) IO ()
firstForward :: MultiCursor e k v -> FreeT (KeyValues k (Producer v IO)) IO ()
firstForward cur = FreeT $ fmap go (first cur)
  where
  go m = case m of
    Nothing -> Pure ()
    Just (KeyValue k v) -> do
      Free $ KeyValues k $ do
        forwardValues cur
        return $ FreeT $ fmap go (nextKey cur)
  -- go = FreeT $ do
  --   m <- nextKey
  --   case m of
  --     Nothing -> Pure ()
  --     Just (KeyValue k v) -> do
  --       Free $ KeyValue k $ do
  --         forwardValues
  --         go

first :: MultiCursor e k v -> IO (Maybe (KeyValue k v))
first mc = getWithoutKey MDB_FIRST (downgradeCursor mc)

-- | The next key and its first value.
nextKey :: MultiCursor e k v -> IO (Maybe (KeyValue k v))
nextKey = error "write me"

-- | The previous key and its last value.
prevKey :: MultiCursor e k v -> IO (Maybe (KeyValue k v))
prevKey = error "write me"

-- | The next value of the current key. This does not venture
--   into the next key if the cursor is positioned at the last value
--   of the current key.
nextValue :: MultiCursor e k v -> IO (Maybe v)
nextValue = error "write me"

lookupFirstValue :: MultiCursor e k v -> k -> IO (Maybe v)
lookupFirstValue mc k = getValueWithKey MDB_SET_KEY (downgradeCursor mc) k

-- | Lookup all values at the given key. These values are provided
--   as a 'Producer' since there can be many pages of values. Since
--   the resulting 'Producer' captures the 'Cursor' given as the
--   first argument, it should not escape the call to 'withCursor' in
--   which the 'Cursor' was bound. Additionally, the 'Producer' should
--   be consumed before any other functions that use the 'Cursor' are
--   called. It is fine if the 'Producer' is not consumed entirely, as
--   long as the caller is not dependending on the cursor to end in a
--   particular place.
--
--   If values are encoded and decoded by a 'Codec' that uses a fixed
--   length, this function will take advantage of @MDB_GET_MULTIPLE@
--   and @MDB_NEXT_MULTIPLE@.
--
lookupValues :: MultiCursor e k v -> k -> Producer' v IO ()
lookupValues cur k = do
  m <- lift $ lookupFirstValue cur k
  case m of
    Nothing -> return ()
    Just v -> do
      yield v
      forwardValues cur

-- | Stream all values associated with the key, starting with the value
--   after the cursor\'s current position.
forwardValues :: MultiCursor e k v -> Producer' v IO ()
forwardValues cur = if isFixed
  then error "implement dupfixed value iteration"
  else forwardValuesStandard cur
  where
  isFixed = case multiCursorDatabaseSettings cur of
    MultiDatabaseSettings _ _ _ _ encVal _ -> isEncodingDupFixed encVal

forwardValuesStandard :: MultiCursor e k v -> Producer' v IO ()
forwardValuesStandard (MultiCursor cur dbs) = go where
  go = do
    m <- lift $ withKVPtrsNoInit $ \keyPtr valPtr -> do
      success <- mdb_cursor_get_X MDB_NEXT_DUP cur keyPtr valPtr
      decodeOne (getDecoding $ multiDatabaseSettingsDecodeValue dbs) success valPtr
    case m of
      Nothing -> return ()
      Just v -> yield v >> go

-- | Insert a key-value pair. If the value already exists at the key, do not add
--   another copy of it. This treats the existing values corresponding
--   to each key as a set. This uses @MDB_NODUPDATA@.
insert :: MultiCursor 'ReadWrite k v -> k -> v -> IO ()
insert cur k v = do
  insertInternalCursorNeutral noDupDataFlags (Right $ downgradeCursor cur) k v
  return ()

-- | Insert a key-value pair. If the value already exists at the key, add another
--   copy of it. This treats the existing values corresponding
--   to each key as a bag.
dupsert :: MultiCursor 'ReadWrite k v -> k -> v -> IO ()
dupsert cur k v = do
  insertInternalCursorNeutral noWriteFlags (Right $ downgradeCursor cur) k v
  return ()

-- | Deletes all the value the cursor is positioned at.
delete :: MultiCursor 'ReadWrite k v -> IO ()
delete (MultiCursor cur _) = mdb_cursor_del_X noWriteFlags cur

-- | Deletes all the values at the key the cursor is positioned at.
deleteValues :: MultiCursor 'ReadWrite k v -> IO ()
deleteValues (MultiCursor cur _) = mdb_cursor_del_X noDupDataFlags cur

-- | Starts at the first element, iterating over all the key-value pairs
--   in order and deleting the ones for which the client requests 'False'.
-- filterServer :: forall k v. MultiCursor 'ReadWrite k v -> Server' Bool (KeyValue k v) IO ()
-- filterServer (MultiCursor cur _) = go where
--   go :: KeyValue k v -> Server' Bool (KeyValue k v) IO ()
--   go kv = do
--     isDeleted <- respond kv
--     when isDeleted $ delete cur

iterT :: (Functor f, Monad m) => (f (m a) -> m a) -> FreeT f m a -> m a
iterT f (FreeT m) = do
    val <- m
    case fmap (iterT f) val of
        Pure x -> return x
        Free y -> f y

hoistFreeT :: (Monad m, Functor f) => (forall a. m a -> n a) -> FreeT f m b -> FreeT f n b
hoistFreeT mh = FreeT . mh . liftM (fmap (hoistFreeT mh)) . runFreeT

-- deleteBy :: MultiCursor 'ReadWrite k v -> (k -> v -> Bool) -> IO ()
-- deleteBy (MultiCursor cur dbs) = go where
--   go = do
--     m <- lift $ withKVPtrsNoInit $ \keyPtr valPtr -> do
--       success <- mdb_cursor_get_X MDB_NEXT_DUP cur keyPtr valPtr
--       decodeOne (getDecoding $ multiDatabaseSettingsDecodeValue dbs) success valPtr
--     case m of
--       Nothing -> return ()
--       Just v -> yield v >> go



