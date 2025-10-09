package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector._

abstract class IntegralArray[Value : ValueCodec, ArrowVector <: FieldVector, Arr <: IntegralArray[Value, ArrowVector, Arr]](
  vector: ArrowVector
) extends NumericArray.Typed[Value, ArrowVector, Arr](vector) {

}

object IntegralArray {
  class BooleanArray(vector: BitVector) extends IntegralArray[Boolean, BitVector, BooleanArray](vector) {
    // Accessors
    override def get(index: Int): Boolean = {
      vector.get(index) != 0
    }

    // Mutators
    override def set(index: Int, value: Boolean): this.type = {
      vector.set(index, if (value) 1 else 0)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toBoolean(value))
    }
  }

  class ByteArray(vector: TinyIntVector) extends IntegralArray[Byte, TinyIntVector, ByteArray](vector) {
    // Accessors
    override def get(index: Int): Byte = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toByte(value))
    }
  }

  class UByteArray(vector: UInt1Vector) extends IntegralArray[UByte, UInt1Vector, UByteArray](vector) {
    // Accessors
    override def get(index: Int): UByte = {
      UByte.trunc(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toUByte(value))
    }
  }

  class ShortArray(vector: SmallIntVector) extends IntegralArray[Short, SmallIntVector, ShortArray](vector) {
    // Accessors
    override def get(index: Int): Short = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toShort(value))
    }
  }

  class UShortArray(vector: UInt2Vector) extends IntegralArray[Char, UInt2Vector, UShortArray](vector) {
    // Accessors
    override def get(index: Int): Char = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Char): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toChar(value))
    }
  }

  class IntArray(vector: IntVector) extends IntegralArray[Int, IntVector, IntArray](vector) {
    // Accessors
    override def get(index: Int): Int = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toInt(value))
    }
  }

  class UIntArray(vector: UInt4Vector) extends IntegralArray[UInt, UInt4Vector, UIntArray](vector) {
    // Accessors
    override def get(index: Int): UInt = {
      UInt.unsafe(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: UInt): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toUInt(value))
    }
  }

  class LongArray(vector: BigIntVector) extends IntegralArray[Long, BigIntVector, LongArray](vector) {
    // Accessors
    override def get(index: Int): Long = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toLong(value))
    }
  }

  class ULongArray(vector: UInt8Vector) extends IntegralArray[ULong, UInt8Vector, ULongArray](vector) {
    // Accessors
    override def get(index: Int): ULong = {
      ULong.trunc(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toULong(value))
    }
  }

  class TimestampArray(vector: TimeStampVector) extends IntegralArray[Long, TimeStampVector, TimestampArray](vector) {
    // Accessors
    override def get(index: Int): Long = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toLong(value))
    }
  }

  class DateDayArray(vector: DateDayVector) extends IntegralArray[Int, DateDayVector, DateDayArray](vector) {
    // Accessors
    override def get(index: Int): Int = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toInt(value))
    }
  }
}
