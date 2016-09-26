{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

module Lmdb.Codec where

import Data.Word
import Lmdb.Types
import Foreign.Storable (peek,poke)
import Foreign.Ptr (Ptr,castPtr,plusPtr)
import Foreign.C.Types (CSize(..))
import Data.Text.Internal (Text(..))
import GHC.Int (Int64(I64#))
import GHC.Ptr (Ptr(Ptr))
import GHC.Types (IO(IO),Int(I#))
import Data.Bits (unsafeShiftR,unsafeShiftL)
import qualified Data.Text.Array as TextArray
import GHC.Prim (newByteArray#,copyAddrToByteArray#,unsafeFreezeByteArray#,copyByteArrayToAddr#)

text :: Codec Text
text = Codec encodeText decodeText

word :: Codec Word
word = Codec encodeIntegral decodeIntegral

word64 :: Codec Word64
word64 = Codec encodeWord64 decodeWord64

int :: Codec Int
int = Codec encodeIntegral decodeIntegral

int64 :: Codec Int
int64 = Codec encodeIntegral decodeIntegral

decodeText :: CSize -> Ptr Word8 -> IO Text
decodeText (CSize szWord64) (Ptr addr) =
  let !szInt@(I# sz) = fromIntegral szWord64
   in IO (\ s1 ->
          case newByteArray# sz s1 of
            (# s2, mutByteArr #) -> case copyAddrToByteArray# addr mutByteArr 0# sz s2 of
              s3 -> case unsafeFreezeByteArray# mutByteArr s3 of
                (# s4, byteArr #) -> (# s4, Text (TextArray.Array byteArr) 0 (unsafeShiftR szInt 1) #)
          )

encodeText :: Text -> Encoding
encodeText (Text arr off len) = Encoding (CSize $ fromIntegral $ unsafeShiftL len 1)
  $ \ptr -> fastMemcpyTextArray arr (castPtr ptr) off len

fastMemcpyTextArray :: TextArray.Array -> Ptr a -> Int -> Int -> IO ()
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

encodeIntegral :: Integral a => a -> Encoding
encodeIntegral = encodeWord64 . fromIntegral
{-# INLINE encodeIntegral #-}

decodeIntegral :: Integral a => CSize -> Ptr Word8 -> IO a
decodeIntegral sz = fmap fromIntegral . decodeWord64 sz
{-# INLINE decodeIntegral #-}

encodeWord32 :: Word32 -> Encoding
encodeWord32 w = Encoding 4 $ \ptr ->
  poke (castPtr ptr :: Ptr Word32) w
{-# INLINE encodeWord32 #-}

encodeWord64 :: Word64 -> Encoding
encodeWord64 w = Encoding 8 $ \ptr ->
  poke (castPtr ptr :: Ptr Word64) w
{-# INLINE encodeWord64 #-}

decodeWord64 :: CSize -> Ptr Word8 -> IO Word64
decodeWord64 sz ptr = if sz == 8
  then peek (castPtr ptr :: Ptr Word64)
  else fail "lmdb failure decoding 64-bit integral type"
{-# INLINE decodeWord64 #-}



