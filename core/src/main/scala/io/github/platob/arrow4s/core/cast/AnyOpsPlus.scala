
package io.github.platob.arrow4s.core.cast

import io.github.platob.arrow4s.core.ArrowRecord
import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}

trait AnyOpsPlus[T] extends Numeric[T] {
  @inline def toString(value: T): String = value.toString
  @inline def fromString(value: String): T

  @inline override def parseString(str: String): Option[T] = {
    try {
      Some(fromString(str))
    } catch {
      case _: Throwable => None
    }
  }

  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toBoolean(value: T): Boolean
  @inline def fromBoolean(value: Boolean): T

  @inline def toByte(value: T): Byte
  @inline def fromByte(value: Byte): T

  @inline def toUByte(value: T): UByte = UByte.trunc(toInt(value))
  @inline def fromUByte(value: UByte): T = fromInt(value.toInt)

  @inline def toShort(value: T): Short
  @inline def fromShort(value: Short): T

  @inline def toUShort(value: T): UShort = UShort.trunc(toInt(value))
  @inline def fromUShort(value: UShort): T = fromInt(value.toInt)

  @inline def toInt(value: T): Int
  @inline def fromInt(value: Int): T

  @inline def toUInt(value: T): UInt = UInt.trunc(toLong(value))
  @inline def fromUInt(value: UInt): T = fromLong(value.toLong)

  @inline def toLong(value: T): Long
  @inline def fromLong(value: Long): T

  @inline def toULong(value: T): ULong = ULong.trunc(toBigInteger(value))
  @inline def fromULong(value: ULong): T = fromBigInteger(value.toBigInteger)

  @inline def toFloat(value: T): Float
  @inline def fromFloat(value: Float): T

  @inline def toDouble(value: T): Double
  @inline def fromDouble(value: Double): T

  @inline def toBigInteger(value: T): java.math.BigInteger
  @inline def fromBigInteger(value: java.math.BigInteger): T

  @inline def toBigDecimal(value: T): java.math.BigDecimal
  @inline def fromBigDecimal(value: java.math.BigDecimal): T

  @inline def toInstant(value: T): java.time.Instant
  @inline def fromInstant(value: java.time.Instant): T

  @inline def toArrowArray(value: T): ArrowArray
  @inline def fromArrowArray(value: ArrowArray): T

  @inline def toArrowRecord(value: T): ArrowRecord
  @inline def fromArrowRecord(value: ArrowRecord): T
}

object AnyOpsPlus {
  trait Logical[L, T] extends AnyOpsPlus[L] {
    @inline def underlying: AnyOpsPlus[T]
    @inline def toLogical(value: T): L
    @inline def fromLogical(value: L): T

    // Numeric
    @inline override def plus(x: L, y: L): L = toLogical(underlying.plus(fromLogical(x), fromLogical(y)))
    @inline override def minus(x: L, y: L): L = toLogical(underlying.minus(fromLogical(x), fromLogical(y)))
    @inline override def times(x: L, y: L): L = toLogical(underlying.times(fromLogical(x), fromLogical(y)))
    @inline override def negate(x: L): L = toLogical(underlying.negate(fromLogical(x)))
    @inline override def compare(x: L, y: L): Int = underlying.compare(fromLogical(x), fromLogical(y))

    // AnyOpsPlus
    @inline override def toString(value: L): String = underlying.toString(fromLogical(value))
    @inline override def fromString(value: String): L = toLogical(underlying.fromString(value))

    @inline override def toBytes(value: L): Array[Byte] = underlying.toBytes(fromLogical(value))
    @inline override def fromBytes(value: Array[Byte]): L = toLogical(underlying.fromBytes(value))

    @inline override def toBoolean(value: L): Boolean = underlying.toBoolean(fromLogical(value))
    @inline override def fromBoolean(value: Boolean): L = toLogical(underlying.fromBoolean(value))

    @inline override def toByte(value: L): Byte = underlying.toByte(fromLogical(value))
    @inline override def fromByte(value: Byte): L = toLogical(underlying.fromByte(value))

    @inline override def toShort(value: L): Short = underlying.toShort(fromLogical(value))
    @inline override def fromShort(value: Short): L = toLogical(underlying.fromShort(value))

    @inline override def toInt(value: L): Int = underlying.toInt(fromLogical(value))
    @inline override def fromInt(value: Int): L = toLogical(underlying.fromInt(value))

    @inline override def toLong(value: L): Long = underlying.toLong(fromLogical(value))
    @inline override def fromLong(value: Long): L = toLogical(underlying.fromLong(value))

    @inline override def toFloat(value: L): Float = underlying.toFloat(fromLogical(value))
    @inline override def fromFloat(value: Float): L = toLogical(underlying.fromFloat(value))

    @inline override def toDouble(value: L): Double = underlying.toDouble(fromLogical(value))
    @inline override def fromDouble(value: Double): L = toLogical(underlying.fromDouble(value))

    @inline override def toBigInteger(value: L): java.math.BigInteger = underlying.toBigInteger(fromLogical(value))
    @inline override def fromBigInteger(value: java.math.BigInteger): L = toLogical(underlying.fromBigInteger(value))

    @inline override def toBigDecimal(value: L): java.math.BigDecimal = underlying.toBigDecimal(fromLogical(value))
    @inline override def fromBigDecimal(value: java.math.BigDecimal): L = toLogical(underlying.fromBigDecimal(value))

    @inline override def toInstant(value: L): java.time.Instant = underlying.toInstant(fromLogical(value))
    @inline override def fromInstant(value: java.time.Instant): L = toLogical(underlying.fromInstant(value))

    @inline override def toArrowArray(value: L): ArrowArray = underlying.toArrowArray(fromLogical(value))
    @inline override def fromArrowArray(value: ArrowArray): L = toLogical(underlying.fromArrowArray(value))

    @inline override def toArrowRecord(value: L): ArrowRecord = underlying.toArrowRecord(fromLogical(value))
    @inline override def fromArrowRecord(value: ArrowRecord): L = toLogical(underlying.fromArrowRecord(value))
  }

  def logical[L, T](to: T => L, from: L => T)(implicit base: AnyOpsPlus[T]): Logical[L, T] =
    new Logical[L, T] {
      override def underlying: AnyOpsPlus[T] = base
      override def toLogical(value: T): L = to(value)
      override def fromLogical(value: L): T = from(value)
    }

  def optional[T](to: T => Option[T], from: Option[T] => T)(implicit base: AnyOpsPlus[T]): Logical[Option[T], T] =
    new Logical[Option[T], T] {
      override def underlying: AnyOpsPlus[T] = base
      override def toLogical(value: T): Option[T] = to(value)
      override def fromLogical(value: Option[T]): T = from(value)
    }
}
