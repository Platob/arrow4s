package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector._

import scala.reflect.runtime.{universe => ru}

trait IntegralArray[V <: FieldVector, T] extends NumericArray.Typed[V, T] {

}

object IntegralArray {
  class BooleanArray(override val vector: BitVector) extends IntegralArray[BitVector, Boolean] {
    // Accessors
    override def get(index: Int): Boolean = {
      vector.get(index) != 0
    }

    // Mutators
    override def set(index: Int, value: Boolean): this.type = {
      vector.set(index, if (value) 1 else 0)
      this
    }
  }

  class ByteArray(override val vector: TinyIntVector) extends IntegralArray[TinyIntVector, Byte] {
    // Accessors
    override def get(index: Int): Byte = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UByteArray(override val vector: UInt1Vector) extends IntegralArray[UInt1Vector, UByte] {
    // Accessors
    override def get(index: Int): UByte = {
      UByte.trunc(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class ShortArray(override val vector: SmallIntVector) extends IntegralArray[SmallIntVector, Short] {
    override val scalaType: ru.Type = ru.typeOf[Short].dealias

    // Accessors
    override def get(index: Int): Short = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UShortArray(override val vector: UInt2Vector) extends IntegralArray[UInt2Vector, Char] {
    // Accessors
    override def get(index: Int): Char = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Char): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class IntArray(val vector: IntVector) extends IntegralArray[IntVector, Int] {
    // Accessors
    override def get(index: Int): Int = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UIntArray(override val vector: UInt4Vector) extends IntegralArray[UInt4Vector, UInt] {
    // Accessors
    override def get(index: Int): UInt = {
      UInt.unsafe(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: UInt): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class LongArray(override val vector: BigIntVector) extends IntegralArray[BigIntVector, Long] {
    // Accessors
    override def get(index: Int): Long = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }
  }

  class ULongArray(override val vector: UInt8Vector) extends IntegralArray[UInt8Vector, ULong] {
    // Accessors
    override def get(index: Int): ULong = {
      ULong.trunc(vector.get(index))
    }

    // Mutators
    override def set(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }
  }

  class TimestampArray(override val vector: TimeStampVector) extends IntegralArray[TimeStampVector, Long] {
    // Accessors
    override def get(index: Int): Long = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }
  }

  class DateDayArray(override val vector: DateDayVector) extends IntegralArray[DateDayVector, Int] {
    // Accessors
    override def get(index: Int): Int = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }
  }
}
