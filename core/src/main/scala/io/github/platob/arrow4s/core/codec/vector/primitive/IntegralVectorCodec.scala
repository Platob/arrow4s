package io.github.platob.arrow4s.core.codec.vector.primitive

import io.github.platob.arrow4s.core.codec.value.primitive.IntegralValueCodec
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector._

trait IntegralVectorCodec[T] extends NumericVectorCodec[T] {

}

object IntegralVectorCodec {
  abstract class Typed[T : IntegralValueCodec, V <: FieldVector]
    extends NumericVectorCodec.Typed[T, V] with IntegralVectorCodec[T] {

  }

  class BooleanVectorCodec extends Typed[Boolean, BitVector] {
    override def get(vector: BitVector, index: Int): Boolean =
      vector.get(index) == 1

    override def set(vector: BitVector, index: Int, value: Boolean): Unit =
      vector.set(index, if (value) 1 else 0)
  }

  class ByteVectorCodec extends Typed[Byte, TinyIntVector] {
    override def get(vector: TinyIntVector, index: Int): Byte =
      vector.get(index)

    override def set(vector: TinyIntVector, index: Int, value: Byte): Unit =
      vector.set(index, value)
  }

  class UByteVectorCodec extends Typed[UByte, UInt1Vector] {
    override def get(vector: UInt1Vector, index: Int): UByte = {
      UByte.unsafe(vector.get(index))
    }

    override def set(vector: UInt1Vector, index: Int, value: UByte): Unit = {
      vector.set(index, value.toByte)
    }
  }

  class ShortVectorCodec extends Typed[Short, SmallIntVector] {
    override def get(vector: SmallIntVector, index: Int): Short =
      vector.get(index)

    override def set(vector: SmallIntVector, index: Int, value: Short): Unit =
      vector.set(index, value)
  }

  class CharVectorCodec extends Typed[Char, UInt2Vector] {
    override def get(vector: UInt2Vector, index: Int): Char =
      vector.get(index).toChar

    override def set(vector: UInt2Vector, index: Int, value: Char): Unit =
      vector.set(index, value.toInt)
  }

  class IntVectorCodec extends Typed[Int, IntVector] {
    override def get(vector: IntVector, index: Int): Int =
      vector.get(index)

    override def set(vector: IntVector, index: Int, value: Int): Unit =
      vector.set(index, value)
  }

  class UIntVectorCodec extends Typed[UInt, UInt4Vector] {
    override def get(vector: UInt4Vector, index: Int): UInt = {
      UInt.unsafe(vector.get(index))
    }

    override def set(vector: UInt4Vector, index: Int, value: UInt): Unit = {
      vector.set(index, value.toInt)
    }
  }

  class LongVectorCodec extends Typed[Long, BigIntVector] {
    override def get(vector: BigIntVector, index: Int): Long =
      vector.get(index)

    override def set(vector: BigIntVector, index: Int, value: Long): Unit =
      vector.set(index, value)
  }

  class ULongVectorCodec extends Typed[ULong, UInt8Vector] {
    override def get(vector: UInt8Vector, index: Int): ULong = {
      ULong.unsafe(vector.get(index))
    }

    override def set(vector: UInt8Vector, index: Int, value: ULong): Unit = {
      vector.set(index, value.toLong)
    }
  }

  class BigIntegerVectorCodec extends Typed[java.math.BigInteger, DecimalVector] {
    override def get(vector: DecimalVector, index: Int): java.math.BigInteger = {
      vector.getObject(index).toBigInteger
    }

    override def set(vector: DecimalVector, index: Int, value: java.math.BigInteger): Unit = {
      vector.setSafe(index, new java.math.BigDecimal(value))
    }
  }
}
