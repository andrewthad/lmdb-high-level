{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE BangPatterns #-}

module Lmdb.Map
  ( -- * Cursor Operations
    -- ** Movements
    -- *** Key and Value
    move
  , first
  , last
  , next
  , prev
  , lookup
  , lookupGte
  , current
    -- *** Only Key
  , nextKey
  , lookupGteKey
  , currentKey
    -- *** Only Value
  , nextValue
  , currentValue
    -- *** No Data
  , first_
  , next_
    -- ** Streaming
  , forward
  , backward
  , firstForward
  , lastBackward
  , lookupForward
  , lookupGteForward
  , serverRequired
  , serverOptional
    -- ** Writing
  , insert
  , insertSuccess
  , repsert
    -- * Cursorless Operations
    -- ** Reading
  , lookup'
    -- ** Writing
  , insert'
  , insertSuccess'
  , repsert'
  ) where

import Prelude hiding (last,lookup)
import Foreign.Ptr (Ptr)
import Lmdb.Internal
import Lmdb.Types
import Foreign.Storable
import Database.LMDB.Raw
import Data.Word
import Control.Monad.Trans.Class
import Pipes (yield, Producer')
import Pipes.Core (respond,Server',(\>\),(/>/),(>+>),(>~>),request,pull,push)
import Foreign.Marshal.Alloc (allocaBytes,alloca)
import Foreign.C.Types (CSize(..))
import Control.Monad
import qualified Pipes.Internal as Pipes

move :: Cursor e k v -> Movement k -> IO (Maybe (KeyValue k v))
move cursor m = case m of
  MovementNext -> next cursor
  MovementPrev -> prev cursor
  MovementFirst -> first cursor
  MovementLast -> last cursor
  MovementAt k -> lookup cursor k
  MovementAtGte k -> lookupGte cursor k
  MovementCurrent -> currentMaybe cursor


first :: Cursor e k v -> IO (Maybe (KeyValue k v))
first = getWithoutKey MDB_FIRST

first_ :: Cursor e k v -> IO Bool
first_ = getWithoutKey_ MDB_FIRST

last :: Cursor e k v -> IO (Maybe (KeyValue k v))
last = getWithoutKey MDB_LAST

next :: Cursor e k v -> IO (Maybe (KeyValue k v))
next = getWithoutKey MDB_NEXT

next_ :: Cursor e k v -> IO Bool
next_ = getWithoutKey_ MDB_NEXT

nextKey :: Cursor e k v -> IO (Maybe k)
nextKey = getKeyWithoutKey MDB_NEXT

nextValue :: Cursor e k v -> IO (Maybe v)
nextValue = getValueWithoutKey MDB_NEXT

prev :: Cursor e k v -> IO (Maybe (KeyValue k v))
prev = getWithoutKey MDB_PREV

prevKey :: Cursor e k v -> IO (Maybe k)
prevKey = getKeyWithoutKey MDB_PREV

current :: Cursor e k v -> IO (KeyValue k v)
current cursor = do
  m <- getWithoutKey MDB_GET_CURRENT cursor
  maybe (fail currentErr) return m

currentValue :: Cursor e k v -> IO v
currentValue cursor = do
  m <- getValueWithoutKey MDB_GET_CURRENT cursor
  maybe (fail currentErr) return m

currentKey :: Cursor e k v -> IO k
currentKey cursor = do
  m <- getKeyWithoutKey MDB_GET_CURRENT cursor
  maybe (fail currentErr) return m

currentMaybe :: Cursor e k v -> IO (Maybe (KeyValue k v))
currentMaybe = getWithoutKey MDB_GET_CURRENT

currentErr :: String
currentErr = concat
  [ "current: Do not call *current* on an LMDB cursor when "
  , "it is in an invalid position. Do not call it before "
  , "calling something like *first* or *at* on the cursor."
  ]

-- | Uses @MDB_SET_KEY@
lookup :: Cursor e k v -> k -> IO (Maybe (KeyValue k v))
lookup = getWithKey MDB_SET_KEY

-- | Uses @MDB_SET_RANGE@
lookupGte :: Cursor e k v -> k -> IO (Maybe (KeyValue k v))
lookupGte = getWithKey MDB_SET_RANGE

-- | Uses @MDB_SET_RANGE@
lookupGteKey :: Cursor e k v -> k -> IO (Maybe k)
lookupGteKey = getKeyWithKey MDB_SET_RANGE

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

lookupForward :: Cursor e k v -> k -> Producer' (KeyValue k v) IO ()
lookupForward cursor k = yieldMaybeThen (flip lookup k) forward cursor

lookupGteForward :: Cursor e k v -> k -> Producer' (KeyValue k v) IO ()
lookupGteForward cursor k = yieldMaybeThen (flip lookupGte k) forward cursor

serverRaw :: forall e k v.
     Cursor e k v
  -> (Cursor e k v -> IO (Maybe (KeyValue k v)))
  -> Server' (Cursor e k v -> IO (Maybe (KeyValue k v))) (KeyValue k v) IO ()
serverRaw cursor initialAction = lift (initialAction cursor) >>= go
  where
  go :: Maybe (KeyValue k v)
     -> Server' (Cursor e k v -> IO (Maybe (KeyValue k v))) (KeyValue k v) IO ()
  go Nothing = return ()
  go (Just kv) = do
    action <- respond kv
    m <- lift (action cursor)
    go m

serverRequired :: forall e k v.
     Cursor e k v
  -> Movement k
  -> Server' (Movement k) (KeyValue k v) IO ()
serverRequired cursor initialMovement = lift (move cursor initialMovement) >>= go
  where
  go :: Maybe (KeyValue k v)
     -> Server' (Movement k) (KeyValue k v) IO ()
  go Nothing = return ()
  go (Just kv) = do
    movement <- respond kv
    m <- lift (move cursor movement)
    go m

-- serverOptional :: forall e k v a.
--      Cursor e k v
--   -> Movement k
--   -> Server' (Movement k) (Maybe (KeyValue k v)) IO a
-- serverOptional cursor initialMovement = lift (move cursor initialMovement) >>= go
--   where
--   go :: Maybe (KeyValue k v) -> Server' (Movement k) (Maybe (KeyValue k v)) IO a
--   go = go <=< lift . move cursor <=< respond

serverOptional :: Cursor e k v -> Movement k -> Server' (Movement k) (Maybe (KeyValue k v)) IO a
serverOptional cursor initialMovement =
  lift (move cursor initialMovement) >>= iterateM (lift . move cursor <=< respond)

iterateM :: Monad m => (a -> m a) -> a -> m b
iterateM f = let f' = f' <=< f in f'

contramapUpstreamAlt :: Monad m => (c -> a) -> (a -> Pipes.Proxy a' a b' b m r) -> c -> Pipes.Proxy a' c b' b m r
contramapUpstreamAlt f k = contramapUpstream f . (k . f)

-- mapDownstream :: Monad m => (b -> c) -> (b -> Pipes.Proxy a' a b' b m r) -> c -> Pipes.Proxy a' a b' c m r
-- mapDownstream f k = k />/ respond . f

-- contramapUpstreamAlt f k = contramapUpstream f . (k . f)

contramapUpstream :: Monad m => (c -> a) -> Pipes.Proxy a' a b' b m r -> Pipes.Proxy a' c b' b m r
contramapUpstream f = go where
  go (Pipes.Request a' g) = Pipes.Request a' (go . g . f)
  go (Pipes.Respond b g) = Pipes.Respond b (go . g)
  go (Pipes.M m) = Pipes.M (m >>= return . go)
  go (Pipes.Pure r) = Pipes.Pure r

contramapUpstreamM :: Monad m => (c -> m a) -> Pipes.Proxy a' a b' b m r -> Pipes.Proxy a' c b' b m r
contramapUpstreamM f = go where
  go (Pipes.Request a' g) = Pipes.Request a' (Pipes.M . (return . go . g <=< f))
  go (Pipes.Respond b g) = Pipes.Respond b (go . g)
  go (Pipes.M m) = Pipes.M (m >>= return . go)
  go (Pipes.Pure r) = Pipes.Pure r

contramapDownstreamM :: Monad m => (c -> m b') -> Pipes.Proxy a' a b' b m r -> Pipes.Proxy a' a c b m r
contramapDownstreamM f = go where
  go (Pipes.Request a' g) = Pipes.Request a' (go . g)
  go (Pipes.Respond b g) = Pipes.Respond b (Pipes.M . (return . go . g <=< f))
  go (Pipes.M m) = Pipes.M (m >>= return . go)
  go (Pipes.Pure r) = Pipes.Pure r

getKeyWithKey :: MDB_cursor_op -> Cursor e k v -> k -> IO (Maybe k)
getKeyWithKey op (Cursor cur settings) k = do
  let SizedPoke keySize keyPoke = case settings of
        DatabaseSettings _ keyEncoding _ _ _ -> runEncoding keyEncoding k
  allocaBytes (fromIntegral keySize) $ \(keyDataPtr :: Ptr Word8) -> do
    keyPoke keyDataPtr
    withKVPtrsInitKey (MDB_val keySize keyDataPtr) $ \keyPtr valPtr -> do
      success <- mdb_cursor_get_X op cur keyPtr valPtr
      decodeOne (getDecoding $ databaseSettingsDecodeKey settings) success keyPtr

getWithoutKey :: MDB_cursor_op -> Cursor e k v -> IO (Maybe (KeyValue k v))
getWithoutKey op (Cursor cur settings) = do
  withKVPtrsNoInit $ \(keyPtr :: Ptr MDB_val) (valPtr :: Ptr MDB_val) -> do
    success <- mdb_cursor_get_X op cur keyPtr valPtr
    decodeResults settings success keyPtr valPtr

getWithoutKey_ :: MDB_cursor_op -> Cursor e k v -> IO Bool
getWithoutKey_ op (Cursor cur settings) = do
  withKVPtrsNoInit $ \(keyPtr :: Ptr MDB_val) (valPtr :: Ptr MDB_val) -> do
    mdb_cursor_get_X op cur keyPtr valPtr

getKeyWithoutKey :: MDB_cursor_op -> Cursor e k v -> IO (Maybe k)
getKeyWithoutKey op (Cursor cur settings) = do
  withKVPtrsNoInit $ \(keyPtr :: Ptr MDB_val) (valPtr :: Ptr MDB_val) -> do
    success <- mdb_cursor_get_X op cur keyPtr valPtr
    decodeOne (getDecoding $ databaseSettingsDecodeKey settings) success keyPtr

lookup' :: Transaction 'ReadOnly -> Database k v -> k -> IO (Maybe v)
lookup' = lookupInternal

impossibleFailure :: String -> IO a
impossibleFailure funcName = fail $ concat
  [ "LMDB "
  , funcName
  , ": This operation failed, although this should not "
  , "be possible unless the datastore has filled up."
  ]

-- | Insert a value at the given key, replacing a previously existing value.
repsert' :: Transaction 'ReadWrite -> Database k v -> k -> v -> IO ()
repsert' a b c d = do
  success <- insertInternal noWriteFlags a b c d
  when (not success) $ impossibleFailure "repsert'"

-- | Insert a value at the given key, throwing an exception if a value
--   already exists at this key.
insert' :: Transaction 'ReadWrite -> Database k v -> k -> v -> IO ()
insert' a b c d = do
  success <- insertInternal noOverwriteFlags a b c d
  when (not success) $ fail "LMDB insert': a value already exists at this key"

-- | Insert a value at the given key, returning 'True' if the operation succeeds
--   and 'False' if a value already exists at this key.
insertSuccess' :: Transaction 'ReadWrite -> Database k v -> k -> v -> IO Bool
insertSuccess' = insertInternal noOverwriteFlags

insert :: Cursor 'ReadWrite k v -> k -> v -> IO ()
insert cur k v = do
  success <- insertInternalCursorNeutral noOverwriteFlags (Right cur) k v
  when (not success) $ fail "LMDB insert: a value already exists at this key"

repsert :: Cursor 'ReadWrite k v -> k -> v -> IO ()
repsert cur k v = do
  success <- insertInternalCursorNeutral noWriteFlags (Right cur) k v
  when (not success) $ impossibleFailure "repsert"

insertSuccess :: Cursor 'ReadWrite k v -> k -> v -> IO Bool
insertSuccess cur k v = insertInternalCursorNeutral noOverwriteFlags (Right cur) k v


