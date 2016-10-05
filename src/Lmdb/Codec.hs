{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Lmdb.Codec where

import Data.Word
import Control.Monad
import Lmdb.Types
import Foreign.Storable (peek,poke,sizeOf)
import Foreign.Ptr (Ptr,castPtr,plusPtr)
import Foreign.C.Types (CSize(..))
import Data.Text.Internal (Text(..))
import GHC.Int (Int64(I64#))
import GHC.Ptr (Ptr(Ptr))
import GHC.Types (IO(IO),Int(I#))
import Data.Bits (unsafeShiftR,unsafeShiftL)
import qualified Data.Text.Array as TextArray
import GHC.Prim (newByteArray#,copyAddrToByteArray#,unsafeFreezeByteArray#,copyByteArrayToAddr#)
import Data.ByteString.Internal (ByteString(PS))
import Foreign.Marshal.Utils (copyBytes)
import Foreign.ForeignPtr (withForeignPtr,mallocForeignPtrBytes)
import qualified Data.Vector.Unboxed as UVector
import qualified Data.Vector.Unboxed.Mutable as MUVector
import qualified Data.Vector.Primitive as PVector
import Data.Primitive.Types (Prim)
import Data.Vector.Unboxed (Unbox)
import Foreign.Storable (Storable,sizeOf,peekElemOff)
import Control.Monad.Trans.State.Strict (StateT(runStateT),get,put)
import Control.Monad.Trans.Class

text :: Codec 'Variable Text
text = Codec encodeText (Decoding decodeText)

word :: Codec 'MachineWord Word
word = Codec unsafeEncodePaddedIntegral unsafeDecodePaddedIntegral

int :: Codec 'MachineWord Int
int = Codec unsafeEncodePaddedIntegral unsafeDecodePaddedIntegral

word32Padded :: Codec 'MachineWord Word32
word32Padded = Codec unsafeEncodePaddedIntegral unsafeDecodePaddedIntegral

word64 :: Codec 'Fixed Word64
word64 = Codec encodeStorable (decodeStorable "Word64")

word16 :: Codec 'Fixed Word16
word16 = Codec encodeStorable (decodeStorable "Word16")

-- multiWord :: MultiEncoding 'Fixed Word
-- multiWord = MultiEncodingIndexedTraversal 8

plusPtrTyped :: Ptr a -> Int -> Ptr a
plusPtrTyped = plusPtr

-- -- | Nothing checks to make sure that @a@ is represented as a machine word.
-- unsafeMachineUVectorEncoding :: forall a. (Storable a, Unbox a) => MultiEncoding 'MachineWord a
-- unsafeMachineUVectorEncoding =
--   MultiEncodingUnboxedVector SomeFixedSizeMachineWord slowCopyUVectorToPtr
--
-- -- | Nothing checks to make sure that @a@ is represented as a machine word.
-- unsafeMachineUVectorDecoding :: forall a. (Storable a, Unbox a) => MultiDecoding 'MachineWord a
-- unsafeMachineUVectorDecoding =
--   MultiDecodingUnboxedVector SomeFixedSizeMachineWord slowCopyPtrToUVector
--
-- storableUVectorEncoding :: forall a. (Storable a, Unbox a) => MultiEncoding 'Fixed a
-- storableUVectorEncoding =
--   MultiEncodingUnboxedVector (SomeFixedSizeFixed (fromIntegral $ sizeOf (undefined :: a))) slowCopyUVectorToPtr
--
-- storableUVectorDecoding :: forall a. (Storable a, Unbox a) => MultiDecoding 'Fixed a
-- storableUVectorDecoding =
--   MultiDecodingUnboxedVector (SomeFixedSizeFixed (fromIntegral $ sizeOf (undefined :: a))) slowCopyPtrToUVector
--
-- unsafePaddedUVectorEncoding :: forall a. (Integral a, Unbox a) => MultiEncoding 'MachineWord a
-- unsafePaddedUVectorEncoding = MultiEncodingUnboxedVector SomeFixedSizeMachineWord $ \v ptrWord8 -> do
--   let ptr = castPtr ptrWord8 :: Ptr Word
--       sz = sizeOf (undefined :: Word)
--   UVector.imapM_ (\ix a -> poke (plusPtrTyped ptr (ix * sz)) (fromIntegral a)) v
--
-- unsafePaddedUVectorDecoding :: forall a. (Integral a, Unbox a) => MultiDecoding 'MachineWord a
-- unsafePaddedUVectorDecoding = MultiDecodingUnboxedVector SomeFixedSizeMachineWord $ \len ptrWord8 -> do
--   let ptr = castPtr ptrWord8 :: Ptr Word
--   mv <- MUVector.new len
--   _ <- flip runStateT 0 $ whileM_ (fmap (< len) get) $ do
--     ix <- get
--     put (ix + 1)
--     w <- lift $ peekElemOff ptr ix
--     let a = fromIntegral w
--         highBitsSet = w /= fromIntegral a
--     when highBitsSet $ lift $ fail $ concat
--       [ "LMDB unsafePaddedUVectorDecoding: high bits were set in small integral type. "
--       , "Using a padded representation for values is not advised."
--       ]
--     lift $ MUVector.write mv ix a
--   UVector.unsafeFreeze mv
--
-- slowCopyUVectorToPtr :: forall a. (Storable a, Unbox a) => UVector.Vector a -> Ptr Word8 -> IO ()
-- slowCopyUVectorToPtr v ptrWord8 = do
--   let ptr = castPtr ptrWord8 :: Ptr a
--       sz = sizeOf (undefined :: a)
--   UVector.imapM_ (\ix a -> poke (plusPtrTyped ptr (ix * sz)) a) v
--
-- slowCopyPtrToUVector :: forall a. (Storable a, Unbox a) => Int -> Ptr Word8 -> IO (UVector.Vector a)
-- slowCopyPtrToUVector len ptrWord8 = do
--   let ptr = castPtr ptrWord8 :: Ptr a
--   mv <- MUVector.new len
--   _ <- flip runStateT 0 $ whileM_ (fmap (< len) get) $ do
--     ix <- get
--     put (ix + 1)
--     a <- lift $ peekElemOff ptr ix
--     lift $ MUVector.write mv ix a
--   UVector.unsafeFreeze mv

-- constCopyPtrToUVector :: forall a.
--   Word8 -> a -> Int -> Ptr Word8 -> IO ByteArray
-- constCopyPtrToUVector _expected a len _ = return (PVector.replicate len a)
--
-- constCopyUVectorToPtr :: forall a.
--   Word8 -> PVector.Vector a -> Ptr Word8 -> IO ()
-- constCopyUVectorToPtr w8 v ptr = do
--   let v2 = PVector.imap (\ix _ -> ix) v
--   PVector.mapM_ (\ix -> poke (plusPtrTyped ptr ix) w8) v2

-- multiWordEncoding :: ((Word -> IO b) -> f Word -> IO ()) -> f Word -> Ptr Word8 -> IO ()
-- multiWordEncoding theMapM_ t ptrWord8 = do
--   let ptrWord = castPtr ptrWord8 :: Ptr Word
--   theMapM_ (\ix w -> ) t

-- multiWordEncoding :: ((Word -> IO b) -> f Word -> IO ()) -> f Word -> Ptr Word8 -> IO ()
-- multiWordEncoding theMapM_ t ptrWord8 = do
--   let ptrWord = castPtr ptrWord8 :: Ptr Word
--   theMapM_ (\ix w -> ) t

-- int :: Codec Int
-- int = Codec encodeIntegral decodeIntegral
--
-- int64 :: Codec Int64
-- int64 = Codec encodeIntegral decodeIntegral

byteString :: Codec 'Variable ByteString
byteString = Codec encodeByteString (Decoding decodeByteString)

-- | This may only be used for the value. LMDB does not support
--   zero-length keys.
unit :: Codec 'Fixed ()
unit = Codec encodeEmpty (decodeConst ())

encodeEmpty :: Encoding 'Fixed a
encodeEmpty = EncodingFixed 0 $ \_ -> FixedPoke $ \_ -> return ()

-- multiEncodeEmpty :: MultiEncoding 'Fixed a
-- multiEncodeEmpty = MultiEncodingUnboxedVector (SomeFixedSizeFixed 1) (constCopyUVectorToPtr 0)
--
-- multiDecodeEmpty :: a -> MultiDecoding 'Fixed a
-- multiDecodeEmpty a = MultiDecodingUnboxedVector (SomeFixedSizeFixed 1) (constCopyPtrToUVector 0 a)

decodeConst :: a -> Decoding a
decodeConst a = Decoding $ \sz _ -> if sz == 0
  then return a
  else fail "decodeConst: encountered a non-zero size"

throughByteString :: (a -> ByteString) -> (ByteString -> Maybe a) -> Codec 'Variable a
throughByteString encode decode = Codec
  (encodeThroughByteString encode)
  (Decoding
    (\sz ptr -> do
      bs <- decodeByteString sz ptr
      case decode bs of
        Just a -> return a
        Nothing -> fail "throughByteString: failed while decoding LMDB data"
    )
  )

encodeByteString :: Encoding 'Variable ByteString
encodeByteString = encodeThroughByteString id

encodeThroughByteString :: (a -> ByteString) -> Encoding 'Variable a
encodeThroughByteString f =
  EncodingVariable $ \a -> let (PS fptr off len) = f a in SizedPoke
    (fromIntegral len)
    (\targetPtr -> withForeignPtr fptr $ \sourcePtr ->
        fastMemcpyBytePtr sourcePtr targetPtr off len
    )
{-# INLINE encodeThroughByteString #-}

decodeByteString :: CSize -> Ptr Word8 -> IO ByteString
decodeByteString sz source = do
  let szInt = fromIntegral sz
  fptr <- mallocForeignPtrBytes szInt
  withForeignPtr fptr $ \target -> do
    fastMemcpyBytePtr source target 0 szInt
  return (PS fptr 0 szInt)

-- decodeThroughByteString :: (ByteString -> Maybe a) -> Decoding ByteString
-- decodeThroughByteString f = Decoding $ \sz source = do
--   let szInt = fromIntegral sz
--   fptr <- mallocForeignPtrBytes szInt
--   withForeignPtr fptr $ \target -> do
--     fastMemcpyBytePtr source target 0 szInt
--   return (PS fptr 0 szInt)

decodeText :: CSize -> Ptr Word8 -> IO Text
decodeText (CSize szWord64) (Ptr addr) =
  let !szInt@(I# sz) = fromIntegral szWord64
   in IO (\ s1 ->
          case newByteArray# sz s1 of
            (# s2, mutByteArr #) -> case copyAddrToByteArray# addr mutByteArr 0# sz s2 of
              s3 -> case unsafeFreezeByteArray# mutByteArr s3 of
                (# s4, byteArr #) -> (# s4, Text (TextArray.Array byteArr) 0 (unsafeShiftR szInt 1) #)
          )

encodeText :: Encoding 'Variable Text
encodeText = EncodingVariable $ \(Text arr off len) -> SizedPoke
  (CSize $ fromIntegral $ unsafeShiftL len 1)
  (\ptr -> fastMemcpyTextArray arr (castPtr ptr) off len)

fastMemcpyBytePtr :: Ptr Word8 -> Ptr Word8 -> Int -> Int -> IO ()
fastMemcpyBytePtr source target off len =
  copyBytes target (plusPtr source off :: Ptr Word8) len

fastMemcpyTextArray :: TextArray.Array -> Ptr Word8 -> Int -> Int -> IO ()
fastMemcpyTextArray (TextArray.Array byteArr) (Ptr addr) off len =
  let !(I# offWord8) = unsafeShiftL off 1
      !(I# lenWord8) = unsafeShiftL len 1
   in IO (\ s1 -> case copyByteArrayToAddr# byteArr offWord8 addr lenWord8 s1 of
        s2 -> (# s2, () #)
      )

slowMemcpyTextArray :: TextArray.Array -> Ptr Word16 -> Int -> Int -> IO ()
slowMemcpyTextArray arr ptr off len = go off
  where
  end = off + len
  go !ix
    | ix >= end = return ()
    | otherwise = do
        let w16 = TextArray.unsafeIndex arr ix
        poke (plusPtr ptr (unsafeShiftL ix 1) :: Ptr Word16) w16
        go (ix + 1)

-- unsafeEncodePaddedIntegral :: Integral a => Encoding 'MachineWord a
-- unsafeEncodePaddedIntegral = encodeWord . fromIntegral
-- {-# INLINE unsafeEncodePaddedIntegral #-}

unsafeEncodePaddedIntegral :: Integral a => Encoding 'MachineWord a
unsafeEncodePaddedIntegral = EncodingMachineWord $ \w -> FixedPoke $ \ptr ->
  poke (castPtr ptr :: Ptr Word) (fromIntegral w :: Word)
{-# INLINE unsafeEncodePaddedIntegral #-}

unsafeDecodePaddedIntegral :: Integral a => Decoding a
unsafeDecodePaddedIntegral = Decoding $ \sz ptr -> if sz == sizeOfMachineWord
  then fmap fromIntegral (peek (castPtr ptr :: Ptr Word))
  else fail "lmdb failure decoding machine sized word or integral"
{-# INLINE unsafeDecodePaddedIntegral #-}

decodeIntegral :: Integral a => CSize -> Ptr Word8 -> IO a
decodeIntegral sz = fmap fromIntegral . decodeWord64 sz
{-# INLINE decodeIntegral #-}

encodePaddedWord32 :: Encoding 'MachineWord Word32
encodePaddedWord32 = EncodingMachineWord $ \w -> FixedPoke $ \ptr ->
  poke (castPtr ptr :: Ptr Word) (fromIntegral w :: Word)
{-# INLINE encodePaddedWord32 #-}

encodeWord :: Encoding 'MachineWord Word
encodeWord = EncodingMachineWord $ \w -> FixedPoke $ \ptr ->
  poke (castPtr ptr :: Ptr Word) w
{-# INLINE encodeWord #-}

encodeStorable :: forall a. Storable a => Encoding 'Fixed a
encodeStorable = EncodingFixed (fromIntegral $ sizeOf (undefined :: a)) $ \a -> FixedPoke $ \ptr ->
  poke (castPtr ptr :: Ptr a) a
{-# INLINE encodeStorable #-}

encodeWord64 :: Encoding 'Fixed Word64
encodeWord64 = EncodingFixed 8 $ \w -> FixedPoke $ \ptr ->
  poke (castPtr ptr :: Ptr Word64) w
{-# INLINE encodeWord64 #-}

decodeWord64 :: CSize -> Ptr Word8 -> IO Word64
decodeWord64 sz ptr = if sz == 8
  then peek (castPtr ptr :: Ptr Word64)
  else fail "lmdb failure decoding 64-bit integral type"
{-# INLINE decodeWord64 #-}

decodeStorable :: forall a. Storable a => String -> Decoding a
decodeStorable descr = Decoding $ \sz ptr -> if fromIntegral sz == sizeOf (undefined :: a)
  then peek (castPtr ptr :: Ptr a)
  else fail $ "lmdb failure decoding " ++ descr ++ " storable type due to mismatched size"
{-# INLINE decodeStorable #-}

sizeOfMachineWord :: CSize
sizeOfMachineWord = fromIntegral (sizeOf (undefined :: Word))

whileM_ :: (Monad m) => m Bool -> m a -> m ()
whileM_ p f = go
    where go = do
            x <- p
            if x
                then f >> go
                else return ()


