{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
module Lmdb.Types where

import Database.LMDB.Raw
import Foreign.C.Types (CSize(..),CInt)
import Foreign.Ptr (Ptr,FunPtr)
import Data.Word
import Data.Primitive.ByteArray (ByteArray)
import Control.Monad
import qualified Data.Vector.Unboxed as UVector
import qualified Data.Vector.Primitive as PVector

-- | We only use the promoted version of this data type.
data Mode = ReadOnly | ReadWrite

class ModeBool (x :: Mode) where
  modeIsReadOnly :: proxy x -> Bool

instance ModeBool 'ReadOnly where
  modeIsReadOnly _ = True

instance ModeBool 'ReadWrite where
  modeIsReadOnly _ = False

newtype Environment (x :: Mode) = Environment
  { getEnvironment :: MDB_env
  }

newtype Transaction (t :: Mode) = Transaction
  { getTransaction :: MDB_txn
  }

data Cursor (t :: Mode) k v = Cursor
  { cursorRef :: !CursorByFfi
  , cursorDatabaseSettings :: !(DatabaseSettings k v)
  }

data MultiCursor (t :: Mode) k v = MultiCursor
  { multiCursorRef :: !CursorByFfi
  , multiCursorDatabaseSettings :: !(MultiDatabaseSettings k v)
  }


data KeyValue k v = KeyValue
  { keyValueKey :: !k
  , keyValueValue :: !v
  }

data KeyValues k v a = KeyValues
  { keyValuesKey :: !k
  , keyValuesValue :: v a
  }

data FreeF f a b = Pure a | Free (f b)
newtype FreeT f m a = FreeT { runFreeT :: m (FreeF f a (FreeT f m a)) }

instance Functor f => Functor (FreeF f a) where
  fmap _ (Pure a)  = Pure a
  fmap f (Free as) = Free (fmap f as)
  {-# INLINE fmap #-}

instance (Functor f, Monad m) => Functor (FreeT f m) where
  fmap f (FreeT m) = FreeT (liftM f' m) where
    f' (Pure a)  = Pure (f a)
    f' (Free as) = Free (fmap (fmap f) as)

instance (Functor f, Monad m) => Applicative (FreeT f m) where
  pure a = FreeT (return (Pure a))
  {-# INLINE pure #-}
  (<*>) = ap
  {-# INLINE (<*>) #-}


instance (Functor f, Monad m) => Monad (FreeT f m) where
  fail e = FreeT (fail e)
  return = pure
  {-# INLINE return #-}
  FreeT m >>= f = FreeT $ m >>= \v -> case v of
    Pure a -> runFreeT (f a)
    Free w -> return (Free (fmap (>>= f) w))



-- data KeyValues k v = KeyValues
--   { keyValuesKey :: !k
--   , keyValuesValues
--   }

data CursorByFfi
  = CursorSafe !MDB_cursor
  | CursorUnsafe !MDB_cursor'

data DbiByFfi
  = DbiSafe !MDB_dbi
  | DbiUnsafe !MDB_dbi'

data Database k v = Database
  { databaseRef :: !DbiByFfi
  , databaseTheSettings :: !(DatabaseSettings k v)
  }

data MultiDatabase k v = MultiDatabase
  { multiDatabaseRef :: DbiByFfi
  , multiDatabaseTheSettings :: !(MultiDatabaseSettings k v)
  }

data DatabaseSettings k v = forall ks vs. DatabaseSettings
  { databaseSettingsSort :: !(Sort ks k) -- ^ Sorting
  , databaseSettingsEncodeKey :: !(Encoding ks k)
  , databaseSettingsDecodeKey :: !(Decoding k)
  , databaseSettingsEncodeValue :: !(Encoding vs v)
  , databaseSettingsDecodeValue :: !(Decoding v)
  }

data MultiDatabaseSettings k v = forall ks vs. MultiDatabaseSettings
  { multiDatabaseSettingsSortKey :: Sort ks k
  , multiDatabaseSettingsSortValue :: Sort vs v
  , multiDatabaseSettingsEncodeKey :: !(Encoding ks k)
  , multiDatabaseSettingsDecodeKey :: !(Decoding k)
  , multiDatabaseSettingsEncodeValue :: !(Encoding vs v)
  , multiDatabaseSettingsDecodeValue :: !(Decoding v)
  }

data Codec s a = Codec
  { codecEncode :: !(Encoding s a)
  , codecDecode :: !(Decoding a)
  -- , codecMultiEncode :: !(MultiEncoding s a)
  -- , codecMultiDecode :: !(MultiDecoding s a)
  }

newtype Decoding a = Decoding { getDecoding :: CSize -> Ptr Word8 -> IO a }
-- newtype Encoding a = Encoding { getEncoding :: a -> SizedPoke }

-- data SomeFixedSize (s :: Size) where
--   SomeFixedSizeFixed :: CSize -> SomeFixedSize 'Fixed
--   SomeFixedSizeMachineWord :: SomeFixedSize 'MachineWord
--
-- data MultiEncoding (s :: Size) a where
--   MultiEncodingNone :: MultiEncoding 'Variable a
--   MultiEncodingUnboxedVector :: SomeFixedSize s
--     -> (ByteArray -> Ptr Word8 -> IO ()) -- todo: remove this entirely
--     -> MultiEncoding s a
--   -- MultiEncodingIndexedTraversal :: CSize
--   --   -> (forall f b. ((Int -> a -> IO b) -> f a -> IO ()) -> f a -> Ptr Word8 -> IO ())
--   --   -> MultiEncoding 'Fixed a
--
-- data MultiDecoding (s :: Size) a where
--   MultiDecodingNone :: MultiDecoding 'Variable a
--   MultiDecodingUnboxedVector :: SomeFixedSize s
--     -> (Int -> Ptr Word8 -> IO ByteArray) -- todo: remove this entirely
--     -> (forall m b. Monad m => (a -> m b) -> ByteArray -> m ()) -- maybe get rid of this too...
--     -> MultiDecoding s a

data Encoding (s :: Size) a where
  EncodingVariable :: (a -> SizedPoke) -> Encoding 'Variable a
  EncodingFixed :: CSize -> (a -> FixedPoke) -> Encoding 'Fixed a
  EncodingMachineWord :: (a -> FixedPoke) -> Encoding 'MachineWord a

data SizedPoke = SizedPoke
  { sizedPokeSize :: {-# UNPACK #-} !CSize -- ^ size in bytes
  , sizedPokePoke :: !(Ptr Word8 -> IO ())
  }

newtype FixedPoke = FixedPoke { getFixedPoke :: Ptr Word8 -> IO () }

data Size
  = Variable
  | Fixed
  | MachineWord

data NativeSort (s :: Size) where
  NativeSortLexographic :: NativeSort 'Variable
  NativeSortLexographicBackward :: NativeSort 'Variable
  NativeSortInteger :: NativeSort 'MachineWord

data CustomSort a
  = CustomSortSafe (a -> a -> Ordering)
  | CustomSortUnsafe (FunPtr (Ptr MDB_val -> Ptr MDB_val -> IO CInt))

data Sort (s :: Size) a where
  SortNative :: NativeSort s -> Sort s a
  SortCustom :: CustomSort a -> Sort 'Variable a

data Movement k
  = MovementNext
  | MovementPrev
  | MovementFirst
  | MovementLast
  | MovementAt !k
  | MovementAtGte !k
  | MovementCurrent

-- data Action
--   = ActionDelete
--   | ActionUpdate v

-- data SingMode (x :: Mode) where
--   SingReadOnly :: SingMode 'ReadOnly
--   SingReadWrite :: SingMode 'ReadWrite

-- class ImplicitMode (x :: Mode) where
--   implicitMode :: SingMode x
--
-- instance ImplicitMode 'ReadOnly where
--   implicitMode = SingReadOnly
--
-- instance ImplicitMode 'ReadWrite where
--   implicitMode = SingReadWrite

-- equivalent to logical implication
-- type family Implies (a :: Mode) (b :: Mode) :: Bool where
--   Implies 'ReadOnly 'ReadOnly = 'True
--   Implies 'ReadOnly 'ReadWrite = 'False
--   Implies 'ReadWrite 'ReadOnly = 'True
--   Implies 'ReadWrite 'ReadWrite = 'True
--
-- type SubMode a b = Implies a b ~ 'True

