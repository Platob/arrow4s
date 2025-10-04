package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.{DateUnit, TimeUnit}

import scala.reflect.runtime.{universe => ru}

trait IntegralArray[V <: FieldVector, T] extends NumericArray.Typed[V, T] {
  override def getFloat(index: Int): Float = getInt(index).toFloat

  override def getDouble(index: Int): Double = getLong(index).toDouble

  override def setFloat(index: Int, value: Float): this.type = setInt(index, value.toInt)

  override def setDouble(index: Int, value: Double): this.type = setLong(index, value.toLong)
}

object IntegralArray {
  class BooleanArray(override val vector: BitVector) extends IntegralArray[BitVector, Boolean] {
    override val scalaType: ru.Type = ru.typeOf[Boolean].dealias

    override def get(index: Int): Boolean = {
      vector.get(index) != 0
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: Boolean): this.type = {
      vector.set(index, if (value) 1 else 0)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, if (value != 0) 1 else 0)
      this
    }
  }

  class ByteArray(override val vector: TinyIntVector) extends IntegralArray[TinyIntVector, Byte] {
    override val scalaType: ru.Type = ru.typeOf[Byte].dealias

    override def get(index: Int): Byte = {
      vector.get(index)
    }

    override def getInt(index: Int): Int = {
      vector.get(index).toInt
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: Byte): this.type = {
      vector.set(index, value)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value.toByte)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value.toByte)
      this
    }
  }

  class UByteArray(override val vector: UInt1Vector) extends IntegralArray[UInt1Vector, UByte] {
    override val scalaType: ru.Type = ru.typeOf[UByte].dealias

    override def get(index: Int): UByte = {
      UByte.trunc(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: UByte): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, UByte.trunc(value).toInt)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, UByte.trunc(value).toInt)
      this
    }
  }

  class ShortArray(override val vector: SmallIntVector) extends IntegralArray[SmallIntVector, Short] {
    override val scalaType: ru.Type = ru.typeOf[Short].dealias

    override def get(index: Int): Short = {
      vector.get(index)
    }

    override def getInt(index: Int): Int = {
      vector.get(index).toInt
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: Short): this.type = {
      vector.set(index, value)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value.toShort)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value.toShort)
      this
    }
  }

  class UShortArray(override val vector: UInt2Vector) extends IntegralArray[UInt2Vector, UShort] {
    override val scalaType: ru.Type = ru.typeOf[UShort].dealias

    override def get(index: Int): UShort = {
      UShort.fromChar(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: UShort): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, UShort.trunc(value).toInt)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, UShort.trunc(value).toInt)
      this
    }
  }

  class IntArray(val vector: IntVector) extends IntegralArray[IntVector, Int] {
    override val scalaType: ru.Type = ru.typeOf[Int].dealias

    override def get(index: Int): Int = {
      vector.get(index)
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value.toInt)
      this
    }
  }

  class UIntArray(override val vector: UInt4Vector) extends IntegralArray[UInt4Vector, UInt] {
    override val scalaType: ru.Type = ru.typeOf[UInt].dealias

    override def get(index: Int): UInt = {
      UInt.trunc(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: UInt): this.type = {
      vector.set(index, value.toInt)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, UInt.trunc(value).toInt)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, UInt.trunc(value).toInt)
      this
    }
  }

  class LongArray(override val vector: BigIntVector) extends IntegralArray[BigIntVector, Long] {
    override val scalaType: ru.Type = ru.typeOf[Long].dealias

    override def get(index: Int): Long = vector.get(index)

    override def getInt(index: Int): Int = {
      vector.get(index).toInt
    }

    override def getLong(index: Int): Long = {
      vector.get(index)
    }

    override def set(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value.toLong)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value)
      this
    }
  }

  class ULongArray(override val vector: UInt8Vector) extends IntegralArray[UInt8Vector, ULong] {
    override val scalaType: ru.Type = ru.typeOf[ULong].dealias

    override def get(index: Int): ULong = {
      ULong.trunc(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index).toInt
    }

    override def getLong(index: Int): Long = {
      vector.get(index)
    }

    override def set(index: Int, value: ULong): this.type = {
      vector.set(index, value.toLong)
      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, ULong.trunc(value).toLong)
      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, ULong.trunc(value).toLong)
      this
    }
  }

  class TimestampArray(override val vector: TimeStampVector) extends IntegralArray[TimeStampVector, java.time.Instant] {
    override val scalaType: ru.Type = ru.typeOf[java.time.Instant].dealias

    def arrowType: ArrowType.Timestamp = this.field.getFieldType
      .getType.asInstanceOf[ArrowType.Timestamp]

    private val epochToInstant = arrowType.getUnit match {
      case TimeUnit.SECOND => (v: Long) => java.time.Instant.ofEpochSecond(v)
      case TimeUnit.MILLISECOND => (v: Long) => {
        val s = v / 1000
        val ns = (v % 1000) * 1000000

        java.time.Instant.ofEpochSecond(s, ns)
      }
      case TimeUnit.MICROSECOND => (v: Long) => {
        val s = v / 1000000
        val ns = (v % 1000000) * 1000

        java.time.Instant.ofEpochSecond(s, ns)
      }
      case TimeUnit.NANOSECOND => (v: Long) => {
        val s = v / 1000000000
        val ns = v % 1000000000

        java.time.Instant.ofEpochSecond(s, ns)
      }
      case _ => throw new IllegalArgumentException(s"Unsupported timestamp unit: ${arrowType.getUnit}")
    }

    private val epochFromInstant = arrowType.getUnit match {
      case TimeUnit.SECOND => (v: java.time.Instant) => v.getEpochSecond
      case TimeUnit.MILLISECOND => (v: java.time.Instant) => v.toEpochMilli
      case TimeUnit.MICROSECOND => (v: java.time.Instant) => v.getEpochSecond * 1000000 + v.getNano / 1000
      case TimeUnit.NANOSECOND => (v: java.time.Instant) => v.getEpochSecond * 1000000000 + v.getNano
      case _ => throw new IllegalArgumentException(s"Unsupported timestamp unit: ${arrowType.getUnit}")
    }

    override def get(index: Int): java.time.Instant = {
      epochToInstant(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index).toInt
    }

    override def getLong(index: Int): Long = {
      vector.get(index)
    }

    override def set(index: Int, value: java.time.Instant): this.type = {
      vector.set(index, epochFromInstant(value))

      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value.toLong)

      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value)

      this
    }
  }

  class DateDayArray(override val vector: DateDayVector) extends IntegralArray[DateDayVector, java.time.LocalDate] {
    override val scalaType: ru.Type = ru.typeOf[java.time.LocalDate].dealias

    def arrowType: ArrowType.Date = this.field.getFieldType
      .getType.asInstanceOf[ArrowType.Date]

    private val epochToDate = arrowType.getUnit match {
      case DateUnit.MILLISECOND => (v: Int) => java.time.LocalDate.ofEpochDay(v.toLong / 86400000)
      case DateUnit.DAY => (v: Int) => java.time.LocalDate.ofEpochDay(v.toLong)
      case _ => throw new IllegalArgumentException(s"Unsupported date unit: ${arrowType.getUnit}")
    }

    private val epochFromDate = arrowType.getUnit match {
      case DateUnit.MILLISECOND => (v: java.time.LocalDate) => (v.toEpochDay * 86400000).toInt
      case DateUnit.DAY => (v: java.time.LocalDate) => v.toEpochDay.toInt
      case _ => throw new IllegalArgumentException(s"Unsupported date unit: ${arrowType.getUnit}")
    }

    override def get(index: Int): java.time.LocalDate = {
      epochToDate(vector.get(index))
    }

    override def getInt(index: Int): Int = {
      vector.get(index)
    }

    override def getLong(index: Int): Long = {
      vector.get(index).toLong
    }

    override def set(index: Int, value: java.time.LocalDate): this.type = {
      vector.set(index, epochFromDate(value))

      this
    }

    override def setInt(index: Int, value: Int): this.type = {
      vector.set(index, value)

      this
    }

    override def setLong(index: Int, value: Long): this.type = {
      vector.set(index, value.toInt)

      this
    }
  }
}
