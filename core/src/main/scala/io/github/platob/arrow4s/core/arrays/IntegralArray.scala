package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector._

import scala.reflect.runtime.{universe => ru}

trait IntegralArray[V <: FieldVector, T] extends PrimitiveArray.Typed[V, T] {

}

object IntegralArray {
  class ByteArray(override val vector: TinyIntVector) extends IntegralArray[TinyIntVector, Byte] {
    override val scalaType: ru.Type = ru.typeOf[Byte].dealias

    override def get(index: Int): Byte = {
      vector.get(index)
    }

    override def set(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UByteArray(override val vector: UInt1Vector) extends IntegralArray[UInt1Vector, UByte] {
    override val scalaType: ru.Type = ru.typeOf[UByte].dealias

    override def get(index: Int): UByte = {
      UByte.trunc(vector.get(index))
    }

    override def set(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class ShortArray(override val vector: SmallIntVector) extends IntegralArray[SmallIntVector, Short] {
    override val scalaType: ru.Type = ru.typeOf[Short].dealias

    override def get(index: Int): Short = {
      vector.get(index)
    }

    override def set(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UShortArray(override val vector: UInt2Vector) extends IntegralArray[UInt2Vector, UShort] {
    override val scalaType: ru.Type = ru.typeOf[UShort].dealias

    override def get(index: Int): UShort = {
      UShort.fromChar(vector.get(index))
    }

    override def set(index: Int, value: UShort): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class IntArray(val vector: IntVector) extends IntegralArray[IntVector, Int] {
    override val scalaType: ru.Type = ru.typeOf[Int].dealias

    override def get(index: Int): Int = {
      vector.get(index)
    }

    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UIntArray(override val vector: UInt4Vector) extends IntegralArray[UInt4Vector, UInt] {
    override val scalaType: ru.Type = ru.typeOf[UInt].dealias

    override def get(index: Int): UInt = {
      UInt.trunc(vector.get(index))
    }

    override def set(index: Int, value: UInt): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class LongArray(override val vector: BigIntVector) extends IntegralArray[BigIntVector, Long] {
    override val scalaType: ru.Type = ru.typeOf[Long].dealias

    override def get(index: Int): Long = vector.get(index)

    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }
  }

  class ULongArray(override val vector: UInt8Vector) extends IntegralArray[UInt8Vector, ULong] {
    override val scalaType: ru.Type = ru.typeOf[ULong].dealias

    override def get(index: Int): ULong = {
      ULong.trunc(vector.get(index))
    }

    override def set(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }
  }
}
