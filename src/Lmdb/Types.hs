{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE BangPatterns #-}
module Lmdb.Types where

import Database.LMDB.Raw
import Foreign.C.Types (CSize(..))
import Foreign.Ptr (Ptr)
import Data.Word

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
  { cursorRef :: MDB_cursor'
  , cursorDbiSettings :: DbiSettings k v
  }

data KeyValue k v = KeyValue
  { keyValueKey :: !k
  , keyValueValue :: !v
  }

data DbiHandle k v = DbiHandle
  { dbiHandleRef :: {-# UNPACK #-} !MDB_dbi'
  , dbiHandleSettings :: !(DbiSettings k v)
  }

data DbiSettings k v = DbiSettings
  { dbiSettingsSort :: Sort k -- ^ Sorting
  , dbiSettingsEncodeKey :: k -> Encoding
  , dbiSettingsDecodeKey :: CSize -> Ptr Word8 -> IO k
  , dbiSettingsEncodeValue :: v -> Encoding
  , dbiSettingsDecodeValue :: CSize -> Ptr Word8 -> IO v
  }

data Codec a = Codec
  { codecEncode :: a -> Encoding
  , codecDecode :: CSize -> Ptr Word8 -> IO a
  }

data Encoding = Encoding
  { encodingSize :: {-# UNPACK #-} !CSize -- size in bytes
  , encodingPoke :: !(Ptr Word8 -> IO ())
  }

data Sort a
  = SortLexographic
  | SortLexographicBackward
  | SortIntegralIncreasing
  | SortCustom (a -> a -> Ordering)

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

