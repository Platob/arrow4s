package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.{Implicits, NumericEncoder}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector._

import scala.reflect.runtime.{universe => ru}

trait IntegralArray[T, V <: FieldVector] extends PrimitiveArray.Typed[T, V] {

}

object IntegralArray {
  class ByteArray(override val vector: TinyIntVector) extends IntegralArray[Byte, TinyIntVector] {
    override def encoder: NumericEncoder.Arrow[Byte, TinyIntVector] = Implicits.byteOps

    override def getValue(index: Int): Byte = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }

    override def as(castTo: ru.Type): ArrowArray = {
      if (ReflectUtils.isOption(castTo)) {
        val innerType = ReflectUtils.getTypeArgs(castTo).head

        if (innerType == ru.typeOf[Byte]) {
          new ByteArray(vector)
        } else if (innerType == ru.typeOf[UByte]) {
          new UByteArray(vector.asInstanceOf[UInt1Vector])
        } else {
          super.as(castTo)
        }
      } else if (castTo =:= ru.typeOf[UByte]) {
        new UByteArray(vector.asInstanceOf[UInt1Vector])
      } else {
        super.as(castTo)
      }
    }
  }

  class UByteArray(override val vector: UInt1Vector) extends IntegralArray[UByte, UInt1Vector] {
    override def encoder: NumericEncoder.Arrow[UByte, UInt1Vector] = Implicits.ubyteOps

    override def getValue(index: Int): UByte = {
      encoder.fromInt(vector.get(index))
    }

    override def setValue(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class ShortArray(override val vector: SmallIntVector) extends IntegralArray[Short, SmallIntVector] {
    override def encoder: NumericEncoder.Arrow[Short, SmallIntVector] = Implicits.shortOps

    override def getValue(index: Int): Short = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UShortArray(override val vector: UInt2Vector) extends IntegralArray[UShort, UInt2Vector] {
    override def encoder: NumericEncoder.Arrow[UShort, UInt2Vector] = Implicits.ushortOps

    override def getValue(index: Int): UShort = {
      UShort.fromChar(vector.get(index))
    }

    override def setValue(index: Int, value: UShort): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class IntArray(val vector: IntVector) extends IntegralArray[Int, IntVector] {
    override def encoder: NumericEncoder.Arrow[Int, IntVector] = Implicits.intOps

    override def getValue(index: Int): Int = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }
  }

  class UIntArray(override val vector: UInt4Vector) extends IntegralArray[UInt, UInt4Vector] {
    override def encoder: NumericEncoder.Arrow[UInt, UInt4Vector] = Implicits.uintOps

    override def getValue(index: Int): UInt = encoder.fromInt(vector.get(index))

    override def setValue(index: Int, value: UInt): this.type = {
      vector.setWithPossibleTruncate(index, value.toInt)
      this
    }
  }

  class LongArray(override val vector: BigIntVector) extends IntegralArray[Long, BigIntVector] {
    override def encoder: NumericEncoder.Arrow[Long, BigIntVector] = Implicits.longOps

    override def getValue(index: Int): Long = vector.get(index)

    override def setValue(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }
  }

  class ULongArray(override val vector: UInt8Vector) extends IntegralArray[ULong, UInt8Vector] {
    override def encoder: NumericEncoder.Arrow[ULong, UInt8Vector] = Implicits.ulongOps

    override def getValue(index: Int): ULong = {
      encoder.fromLong(vector.get(index))
    }

    override def setValue(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }
  }
}
