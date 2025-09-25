package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.{AnyOpsPlus, NumericOpsPlus}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector._

trait IntegralArray[T, V <: FieldVector] extends PrimitiveArray.Typed[T, V] {
  def converter: NumericOpsPlus[T]
}

object IntegralArray {
  class ByteArray(override val vector: TinyIntVector) extends IntegralArray[Byte, TinyIntVector] {
    override def converter: NumericOpsPlus[Byte] = NumericOpsPlus.byteOps

    override def getValue(index: Int): Byte = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toByte(value))

      this
    }
  }

  class UByteArray(override val vector: UInt1Vector) extends IntegralArray[UByte, UInt1Vector] {
    override def converter: NumericOpsPlus[UByte] = NumericOpsPlus.ubyteOps

    override def getValue(index: Int): UByte = {
      converter.fromInt(vector.get(index))
    }

    override def setValue(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toUByte(value))

      this
    }
  }

  class ShortArray(override val vector: SmallIntVector) extends IntegralArray[Short, SmallIntVector] {
    override def converter: NumericOpsPlus[Short] = NumericOpsPlus.shortOps

    override def getValue(index: Int): Short = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toShort(value))

      this
    }
  }

  class UShortArray(override val vector: UInt2Vector) extends IntegralArray[UShort, UInt2Vector] {
    override def converter: NumericOpsPlus[UShort] = NumericOpsPlus.ushortOps

    override def getValue(index: Int): UShort = {
      UShort.fromChar(vector.get(index))
    }

    override def setValue(index: Int, value: UShort): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toUShort(value))

      this
    }
  }

  class IntArray(val vector: IntVector) extends IntegralArray[Int, IntVector] {
    override def converter: NumericOpsPlus[Int] = NumericOpsPlus.intOps

    override def getValue(index: Int): Int = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toInt(value))

      this
    }
  }

  class UIntArray(override val vector: UInt4Vector) extends IntegralArray[UInt, UInt4Vector] {
    override def converter: NumericOpsPlus[UInt] = NumericOpsPlus.uintOps

    override def getValue(index: Int): UInt = converter.fromInt(vector.get(index))

    override def setValue(index: Int, value: UInt): this.type = {
      vector.setWithPossibleTruncate(index, value.toInt)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toUInt(value))

      this
    }
  }

  class LongArray(override val vector: BigIntVector) extends IntegralArray[Long, BigIntVector] {
    override def converter: NumericOpsPlus[Long] = NumericOpsPlus.longOps

    override def getValue(index: Int): Long = vector.get(index)

    override def setValue(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toLong(value))

      this
    }
  }

  class ULongArray(override val vector: UInt8Vector) extends IntegralArray[ULong, UInt8Vector] {
    override def converter: NumericOpsPlus[ULong] = NumericOpsPlus.ulongOps

    override def getValue(index: Int): ULong = {
      converter.fromLong(vector.get(index))
    }

    override def setValue(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toULong(value))

      this
    }
  }
}
