package io.github.platob.arrow4s.core.extensions

import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class TypeExtension[T : ClassTag] {
  def tpe: ru.Type

  val classTag: ClassTag[T] = scala.reflect.classTag[T]

  // Convenience common types
  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toString(value: T): String
  @inline def fromString(value: String): T

  @inline def toBoolean(value: T): Boolean
  @inline def fromBoolean(value: Boolean): T

  @inline def toByte(value: T): Byte
  @inline def fromByte(value: Byte): T

  @inline def toUByte(value: T): UByte = UByte.trunc(toByte(value))
  @inline def fromUByte(value: UByte): T = fromByte(value.toByte)

  @inline def toShort(value: T): Short
  @inline def fromShort(value: Short): T

  @inline def toChar(value: T): Char
  @inline def fromChar(value: Char): T

  @inline def toInt(value: T): Int
  @inline def fromInt(value: Int): T

  @inline def toUInt(value: T): UInt = UInt.trunc(toInt(value))
  @inline def fromUInt(value: UInt): T = fromInt(value.toInt)

  @inline def toLong(value: T): Long
  @inline def fromLong(value: Long): T

  @inline def toULong(value: T): ULong = ULong.trunc(toLong(value))
  @inline def fromULong(value: ULong): T = fromLong(value.toLong)

  @inline def toFloat(value: T): Float
  @inline def fromFloat(value: Float): T

  @inline def toDouble(value: T): Double
  @inline def fromDouble(value: Double): T

  @inline def toBigInteger(value: T): java.math.BigInteger
  @inline def fromBigInteger(value: java.math.BigInteger): T

  @inline def toBigDecimal(value: T): java.math.BigDecimal
  @inline def fromBigDecimal(value: java.math.BigDecimal): T
}

object TypeExtension {
  implicit val boolean: TypeExtension[Boolean] = new TypeExtension[Boolean] {
    val tpe: ru.Type = ru.typeOf[Boolean].dealias

    override def toBytes(value: Boolean): Array[Byte] = if (value) Array(1.toByte) else Array(0.toByte)
    override def fromBytes(value: Array[Byte]): Boolean = value.nonEmpty && value(0) != 0.toByte

    override def toString(value: Boolean): String = value.toString
    override def fromString(value: String): Boolean = {
      value.head match {
        case 't' | 'T' | '1' | 'y' | 'Y' => true
        case 'f' | 'F' | '0' | 'n' | 'N' => false
        case _ => throw new IllegalArgumentException(s"Cannot convert string '$value' to Boolean")
      }
    }

    override def toBoolean(value: Boolean): Boolean = value
    override def fromBoolean(value: Boolean): Boolean = value

    override def toByte(value: Boolean): Byte = if (value) 1.toByte else 0.toByte
    override def fromByte(value: Byte): Boolean = value != 0.toByte

    override def toUByte(value: Boolean): UByte = if (value) UByte.One else UByte.Zero
    override def fromUByte(value: UByte): Boolean = value != UByte.Zero

    override def toShort(value: Boolean): Short = if (value) 1.toShort else 0.toShort
    override def fromShort(value: Short): Boolean = value != 0.toShort

    override def toChar(value: Boolean): Char = if (value) 1.toChar else 0.toChar
    override def fromChar(value: Char): Boolean = value != 0.toChar

    override def toInt(value: Boolean): Int = if (value) 1 else 0
    override def fromInt(value: Int): Boolean = value != 0

    override def toUInt(value: Boolean): UInt = if (value) UInt.One else UInt.Zero
    override def fromUInt(value: UInt): Boolean = value != UInt.Zero

    override def toLong(value: Boolean): Long = if (value) 1L else 0L
    override def fromLong(value: Long): Boolean = value != 0L

    override def toULong(value: Boolean): ULong = if (value) ULong.One else ULong.Zero
    override def fromULong(value: ULong): Boolean = value != ULong.Zero

    override def toFloat(value: Boolean): Float = if (value) 1.0f else 0.0f
    override def fromFloat(value: Float): Boolean = value != 0.0f

    override def toDouble(value: Boolean): Double = if (value) 1.0 else 0.0
    override def fromDouble(value: Double): Boolean = value != 0.0

    override def toBigInteger(value: Boolean): java.math.BigInteger =
      if (value) java.math.BigInteger.ONE else java.math.BigInteger.ZERO
    override def fromBigInteger(value: java.math.BigInteger): Boolean =
      value != java.math.BigInteger.ZERO

    override def toBigDecimal(value: Boolean): java.math.BigDecimal =
      if (value) java.math.BigDecimal.ONE else java.math.BigDecimal.ZERO
    override def fromBigDecimal(value: java.math.BigDecimal): Boolean =
      value != java.math.BigDecimal.ZERO
  }

  implicit val byte: TypeExtension[Byte] = new TypeExtension[Byte] {
    val tpe: ru.Type = ru.typeOf[Byte].dealias

    override def toBytes(value: Byte): Array[Byte] = Array(value)
    override def fromBytes(value: Array[Byte]): Byte = value(0)

    override def toString(value: Byte): String = value.toString
    override def fromString(value: String): Byte = value.toByte

    override def toBoolean(value: Byte): Boolean = boolean.fromByte(value)
    override def fromBoolean(value: Boolean): Byte = boolean.toByte(value)

    override def toByte(value: Byte): Byte = value
    override def fromByte(value: Byte): Byte = value

    override def toUByte(value: Byte): UByte = UByte.trunc(value)
    override def fromUByte(value: UByte): Byte = value.toByte

    override def toShort(value: Byte): Short = value.toShort
    override def fromShort(value: Short): Byte = value.toByte

    override def toChar(value: Byte): Char = value.toChar
    override def fromChar(value: Char): Byte = value.toByte

    override def toInt(value: Byte): Int = value.toInt
    override def fromInt(value: Int): Byte = value.toByte

    override def toUInt(value: Byte): UInt = UInt.trunc(value)
    override def fromUInt(value: UInt): Byte = value.toByte

    override def toLong(value: Byte): Long = value.toLong
    override def fromLong(value: Long): Byte = value.toByte

    override def toULong(value: Byte): ULong = ULong.trunc(value)
    override def fromULong(value: ULong): Byte = value.toByte

    override def toFloat(value: Byte): Float = value.toFloat
    override def fromFloat(value: Float): Byte = value.toByte

    override def toDouble(value: Byte): Double = value.toDouble
    override def fromDouble(value: Double): Byte = value.toByte

    override def toBigInteger(value: Byte): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Byte =
      value.byteValue()

    override def toBigDecimal(value: Byte): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Byte =
      value.byteValue()
  }

  implicit val ubyte: TypeExtension[UByte] = new TypeExtension[UByte] {
    val tpe: ru.Type = ru.typeOf[UByte].dealias

    override def toBytes(value: UByte): Array[Byte] = value.toBytes

    override def fromBytes(value: Array[Byte]): UByte = UByte.unsafe(value(0))

    override def toString(value: UByte): String = value.toString

    override def fromString(value: String): UByte = UByte(value)

    override def toBoolean(value: UByte): Boolean = boolean.fromUByte(value)

    override def fromBoolean(value: Boolean): UByte = boolean.toUByte(value)

    override def toByte(value: UByte): Byte = byte.fromUByte(value)

    override def fromByte(value: Byte): UByte = byte.toUByte(value)

    override def toUByte(value: UByte): UByte = value
    override def fromUByte(value: UByte): UByte = value

    override def toShort(value: UByte): Short = short.fromUByte(value)

    override def fromShort(value: Short): UByte = short.toUByte(value)

    override def toChar(value: UByte): Char = char.fromUByte(value)

    override def fromChar(value: Char): UByte = char.toUByte(value)

    override def toInt(value: UByte): Int = int.fromUByte(value)

    override def fromInt(value: Int): UByte = int.toUByte(value)

    override def toLong(value: UByte): Long = long.fromUByte(value)

    override def fromLong(value: Long): UByte = long.toUByte(value)

    override def toFloat(value: UByte): Float = float.fromUByte(value)

    override def fromFloat(value: Float): UByte = float.toUByte(value)

    override def toDouble(value: UByte): Double = double.fromUByte(value)

    override def fromDouble(value: Double): UByte = double.toUByte(value)

    override def toBigInteger(value: UByte): java.math.BigInteger = value.toBigInteger

    override def fromBigInteger(value: java.math.BigInteger): UByte = UByte.trunc(value.intValue())

    override def toBigDecimal(value: UByte): java.math.BigDecimal = value.toBigDecimal

    override def fromBigDecimal(value: java.math.BigDecimal): UByte = UByte.trunc(value.intValue())
  }

  implicit val short: TypeExtension[Short] = new TypeExtension[Short] {
    val tpe: ru.Type = ru.typeOf[Short].dealias

    override def toBytes(value: Short): Array[Byte] = {
      Array(
        (value >> 8).toByte,
        (value & 0xFF).toByte
      )
    }
    override def fromBytes(value: Array[Byte]): Short = {
      val v = (value(0).toInt << 8) | (value(1).toInt & 0xFF)
      v.toShort
    }

    override def toString(value: Short): String = value.toString
    override def fromString(value: String): Short = value.toShort

    override def toBoolean(value: Short): Boolean = boolean.fromShort(value)
    override def fromBoolean(value: Boolean): Short = boolean.toShort(value)

    override def toByte(value: Short): Byte = byte.fromShort(value)
    override def fromByte(value: Byte): Short = byte.toShort(value)

    override def toUByte(value: Short): UByte = UByte.trunc(value)
    override def fromUByte(value: UByte): Short = value.toShort

    override def toShort(value: Short): Short = value
    override def fromShort(value: Short): Short = value

    override def toChar(value: Short): Char = value.toChar
    override def fromChar(value: Char): Short = value.toShort

    override def toInt(value: Short): Int = value.toInt
    override def fromInt(value: Int): Short = value.toShort

    override def toLong(value: Short): Long = value.toLong
    override def fromLong(value: Long): Short = value.toShort

    override def toFloat(value: Short): Float = value.toFloat
    override def fromFloat(value: Float): Short = value.toShort

    override def toDouble(value: Short): Double = value.toDouble
    override def fromDouble(value: Double): Short = value.toShort

    override def toBigInteger(value: Short): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Short =
      value.shortValue()

    override def toBigDecimal(value: Short): java.math.BigDecimal = {
      java.math.BigDecimal.valueOf(value.toLong)
    }
    override def fromBigDecimal(value: java.math.BigDecimal): Short =
      value.shortValue()
  }

  implicit val char: TypeExtension[Char] = new TypeExtension[Char] {
    val tpe: ru.Type = ru.typeOf[Char].dealias

    override def toBytes(value: Char): Array[Byte] = {
      Array(
        (value >> 8).toByte,
        (value & 0xFF).toByte
      )
    }
    override def fromBytes(value: Array[Byte]): Char = {
      val v = (value(0).toInt << 8) | (value(1).toInt & 0xFF)
      v.toChar
    }

    override def toString(value: Char): String = value.toString
    override def fromString(value: String): Char = value.charAt(0)

    override def toBoolean(value: Char): Boolean = boolean.fromInt(value.toInt)
    override def fromBoolean(value: Boolean): Char = boolean.toInt(value).toChar

    override def toByte(value: Char): Byte = byte.fromChar(value)
    override def fromByte(value: Byte): Char = byte.toChar(value)

    override def toShort(value: Char): Short = short.fromChar(value)
    override def fromShort(value: Short): Char = short.toChar(value)

    override def toChar(value: Char): Char = value
    override def fromChar(value: Char): Char = value

    override def toInt(value: Char): Int = value.toInt
    override def fromInt(value: Int): Char = value.toChar

    override def toLong(value: Char): Long = value.toLong
    override def fromLong(value: Long): Char = value.toChar

    override def toFloat(value: Char): Float = value.toFloat
    override def fromFloat(value: Float): Char = value.toChar

    override def toDouble(value: Char): Double = value.toDouble
    override def fromDouble(value: Double): Char = value.toChar

    override def toBigInteger(value: Char): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Char =
      value.intValue().toChar

    override def toBigDecimal(value: Char): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Char =
      value.intValue().toChar
  }

  implicit val int: TypeExtension[Int] = new TypeExtension[Int] {
    val tpe: ru.Type = ru.typeOf[Int].dealias

    override def toBytes(value: Int): Array[Byte] = {
      Array(
        (value >> 24).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        (value & 0xFF).toByte
      )
    }
    override def fromBytes(value: Array[Byte]): Int = {
      (value(0).toInt << 24) | ((value(1).toInt & 0xFF) << 16) | ((value(2).toInt & 0xFF) << 8) | (value(3).toInt & 0xFF)
    }

    override def toString(value: Int): String = value.toString
    override def fromString(value: String): Int = value.toInt

    override def toBoolean(value: Int): Boolean = boolean.fromInt(value)
    override def fromBoolean(value: Boolean): Int = boolean.toInt(value)

    override def toByte(value: Int): Byte = byte.fromInt(value)
    override def fromByte(value: Byte): Int = byte.toInt(value)

    override def toShort(value: Int): Short = short.fromInt(value)
    override def fromShort(value: Short): Int = short.toInt(value)

    override def toChar(value: Int): Char = char.fromInt(value)
    override def fromChar(value: Char): Int = char.toInt(value)

    override def toInt(value: Int): Int = value
    override def fromInt(value: Int): Int = value

    override def toLong(value: Int): Long = value.toLong
    override def fromLong(value: Long): Int = value.toInt

    override def toFloat(value: Int): Float = value.toFloat
    override def fromFloat(value: Float): Int = value.toInt

    override def toDouble(value: Int): Double = value.toDouble
    override def fromDouble(value: Double): Int = value.toInt

    override def toBigInteger(value: Int): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Int =
      value.intValue()

    override def toBigDecimal(value: Int): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Int =
      value.intValue()
  }

  implicit val uint: TypeExtension[UInt] = new TypeExtension[UInt] {
    val tpe: ru.Type = ru.typeOf[UInt].dealias

    override def toBytes(value: UInt): Array[Byte] = {
      int.toBytes(value.toInt)
    }

    override def fromBytes(value: Array[Byte]): UInt = {
      val i = int.fromBytes(value)
      UInt.unsafe(i)
    }

    override def toString(value: UInt): String = value.toString
    override def fromString(value: String): UInt = UInt.trunc(long.fromString(value))

    override def toBoolean(value: UInt): Boolean = boolean.fromUInt(value)
    override def fromBoolean(value: Boolean): UInt = boolean.toUInt(value)

    override def toByte(value: UInt): Byte = byte.fromUInt(value)
    override def fromByte(value: Byte): UInt = byte.toUInt(value)

    override def toShort(value: UInt): Short = short.fromUInt(value)
    override def fromShort(value: Short): UInt = short.toUInt(value)

    override def toChar(value: UInt): Char = char.fromUInt(value)
    override def fromChar(value: Char): UInt = char.toUInt(value)

    override def toInt(value: UInt): Int = value.toInt
    override def fromInt(value: Int): UInt = UInt.trunc(value)

    override def toLong(value: UInt): Long = value.toLong
    override def fromLong(value: Long): UInt = UInt.trunc(value)

    override def toFloat(value: UInt): Float = value.toFloat
    override def fromFloat(value: Float): UInt = UInt.trunc(value.toInt)

    override def toDouble(value: UInt): Double = value.toDouble
    override def fromDouble(value: Double): UInt = UInt.trunc(value.toLong)

    override def toBigInteger(value: UInt): java.math.BigInteger = value.toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): UInt = UInt.trunc(value.longValue())

    override def toBigDecimal(value: UInt): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): UInt = UInt.trunc(value.longValue())
  }

  implicit val long: TypeExtension[Long] = new TypeExtension[Long] {
    val tpe: ru.Type = ru.typeOf[Long].dealias

    override def toBytes(value: Long): Array[Byte] = {
      Array(
        (value >> 56).toByte,
        ((value >> 48) & 0xFF).toByte,
        ((value >> 40) & 0xFF).toByte,
        ((value >> 32) & 0xFF).toByte,
        ((value >> 24) & 0xFF).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        (value & 0xFF).toByte
      )
    }
    override def fromBytes(value: Array[Byte]): Long = {
      (value(0).toLong << 56) |
        ((value(1).toLong & 0xFF) << 48) |
        ((value(2).toLong & 0xFF) << 40) |
        ((value(3).toLong & 0xFF) << 32) |
        ((value(4).toLong & 0xFF) << 24) |
        ((value(5).toLong & 0xFF) << 16) |
        ((value(6).toLong & 0xFF) << 8) |
        (value(7).toLong & 0xFF)
    }

    override def toString(value: Long): String = value.toString
    override def fromString(value: String): Long = value.toLong

    override def toBoolean(value: Long): Boolean = boolean.fromLong(value)
    override def fromBoolean(value: Boolean): Long = boolean.toLong(value)

    override def toByte(value: Long): Byte = byte.fromLong(value)
    override def fromByte(value: Byte): Long = byte.toLong(value)

    override def toShort(value: Long): Short = short.fromLong(value)
    override def fromShort(value: Short): Long = short.toLong(value)

    override def toChar(value: Long): Char = char.fromLong(value)
    override def fromChar(value: Char): Long = char.toLong(value)

    override def toInt(value: Long): Int = int.fromLong(value)
    override def fromInt(value: Int): Long = int.toLong(value)

    override def toLong(value: Long): Long = value
    override def fromLong(value: Long): Long = value

    override def toFloat(value: Long): Float = value.toFloat
    override def fromFloat(value: Float): Long = value.toLong

    override def toDouble(value: Long): Double = value.toDouble
    override def fromDouble(value: Double): Long = value.toLong

    override def toBigInteger(value: Long): java.math.BigInteger =
      java.math.BigInteger.valueOf(value)
    override def fromBigInteger(value: java.math.BigInteger): Long =
      value.longValue()

    override def toBigDecimal(value: Long): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Long =
      value.longValue()
  }

  implicit val ulong: TypeExtension[ULong] = new TypeExtension[ULong] {
    val tpe: ru.Type = ru.typeOf[ULong].dealias

    override def toBytes(value: ULong): Array[Byte] = {
      long.toBytes(value.toLong)
    }

    override def fromBytes(value: Array[Byte]): ULong = {
      val l = long.fromBytes(value)
      ULong.unsafe(l)
    }

    override def toString(value: ULong): String = value.toString

    override def fromString(value: String): ULong = ULong.trunc(long.fromString(value))

    override def toBoolean(value: ULong): Boolean = boolean.fromULong(value)

    override def fromBoolean(value: Boolean): ULong = boolean.toULong(value)

    override def toByte(value: ULong): Byte = byte.fromULong(value)

    override def fromByte(value: Byte): ULong = byte.toULong(value)

    override def toShort(value: ULong): Short = short.fromULong(value)

    override def fromShort(value: Short): ULong = short.toULong(value)

    override def toChar(value: ULong): Char = char.fromULong(value)

    override def fromChar(value: Char): ULong = char.toULong(value)

    override def toInt(value: ULong): Int = int.fromULong(value)

    override def fromInt(value: Int): ULong = int.toULong(value)

    override def toLong(value: ULong): Long = value.toLong

    override def fromLong(value: Long): ULong = ULong.trunc(value)

    override def toFloat(value: ULong): Float = value.toFloat

    override def fromFloat(value: Float): ULong = ULong.trunc(value.toInt)

    override def toDouble(value: ULong): Double = value.toDouble

    override def fromDouble(value: Double): ULong = ULong.trunc(value.toLong)

    override def toBigInteger(value: ULong): java.math.BigInteger = value.toBigInteger

    override def fromBigInteger(value: java.math.BigInteger): ULong = ULong.trunc(value.longValue())

    override def toBigDecimal(value: ULong): java.math.BigDecimal = value.toBigDecimal

    override def fromBigDecimal(value: java.math.BigDecimal): ULong = ULong.trunc(value.longValue())
  }

  implicit val float: TypeExtension[Float] = new TypeExtension[Float] {
    val tpe: ru.Type = ru.typeOf[Float].dealias

    override def toBytes(value: Float): Array[Byte] = {
      val intBits = java.lang.Float.floatToIntBits(value)

      int.toBytes(intBits)
    }

    override def fromBytes(value: Array[Byte]): Float = {
      val intBits = int.fromBytes(value)
      java.lang.Float.intBitsToFloat(intBits)
    }

    override def toString(value: Float): String = value.toString
    override def fromString(value: String): Float = value.toFloat

    override def toBoolean(value: Float): Boolean = boolean.fromFloat(value)
    override def fromBoolean(value: Boolean): Float = boolean.toFloat(value)

    override def toByte(value: Float): Byte = byte.fromFloat(value)
    override def fromByte(value: Byte): Float = byte.toFloat(value)

    override def toShort(value: Float): Short = short.fromFloat(value)
    override def fromShort(value: Short): Float = short.toFloat(value)

    override def toChar(value: Float): Char = char.fromFloat(value)
    override def fromChar(value: Char): Float = char.toFloat(value)

    override def toInt(value: Float): Int = int.fromFloat(value)
    override def fromInt(value: Int): Float = int.toFloat(value)

    override def toLong(value: Float): Long = long.fromFloat(value)
    override def fromLong(value: Long): Float = long.toFloat(value)

    override def toFloat(value: Float): Float = value
    override def fromFloat(value: Float): Float = value

    override def toDouble(value: Float): Double = value.toDouble
    override def fromDouble(value: Double): Float = value.toFloat

    override def toBigInteger(value: Float): java.math.BigInteger =
      java.math.BigDecimal.valueOf(value.toDouble).toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): Float =
      value.floatValue()

    override def toBigDecimal(value: Float): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toDouble)
    override def fromBigDecimal(value: java.math.BigDecimal): Float =
      value.floatValue()
  }

  implicit val double: TypeExtension[Double] = new TypeExtension[Double] {
    val tpe: ru.Type = ru.typeOf[Double].dealias

    override def toBytes(value: Double): Array[Byte] = {
      val longBits = java.lang.Double.doubleToLongBits(value)
      long.toBytes(longBits)
    }

    override def fromBytes(value: Array[Byte]): Double = {
      val longBits = long.fromBytes(value)
      java.lang.Double.longBitsToDouble(longBits)
    }

    override def toString(value: Double): String = value.toString
    override def fromString(value: String): Double = value.toDouble

    override def toBoolean(value: Double): Boolean = boolean.fromDouble(value)
    override def fromBoolean(value: Boolean): Double = boolean.toDouble(value)

    override def toByte(value: Double): Byte = byte.fromDouble(value)
    override def fromByte(value: Byte): Double = byte.toDouble(value)

    override def toShort(value: Double): Short = short.fromDouble(value)
    override def fromShort(value: Short): Double = short.toDouble(value)

    override def toChar(value: Double): Char = char.fromDouble(value)
    override def fromChar(value: Char): Double = char.toDouble(value)

    override def toInt(value: Double): Int = int.fromDouble(value)
    override def fromInt(value: Int): Double = int.toDouble(value)

    override def toLong(value: Double): Long = long.fromDouble(value)
    override def fromLong(value: Long): Double = long.toDouble(value)

    override def toFloat(value: Double): Float = value.toFloat
    override def fromFloat(value: Float): Double = value.toDouble

    override def toDouble(value: Double): Double = value
    override def fromDouble(value: Double): Double = value

    override def toBigInteger(value: Double): java.math.BigInteger =
      java.math.BigDecimal.valueOf(value).toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): Double =
      value.doubleValue()

    override def toBigDecimal(value: Double): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Double =
      value.doubleValue()
  }

  implicit val bigInteger: TypeExtension[java.math.BigInteger] = new TypeExtension[java.math.BigInteger] {
    val tpe: ru.Type = ru.typeOf[java.math.BigInteger].dealias

    override def toBytes(value: java.math.BigInteger): Array[Byte] =
      value.toByteArray
    override def fromBytes(value: Array[Byte]): java.math.BigInteger =
      new java.math.BigInteger(1, value)

    override def toString(value: java.math.BigInteger): String = value.toString
    override def fromString(value: String): java.math.BigInteger =
      new java.math.BigInteger(value)

    override def toBoolean(value: java.math.BigInteger): Boolean =
      boolean.fromBigInteger(value)
    override def fromBoolean(value: Boolean): java.math.BigInteger =
      boolean.toBigInteger(value)

    override def toByte(value: java.math.BigInteger): Byte =
      byte.fromBigInteger(value)
    override def fromByte(value: Byte): java.math.BigInteger =
      byte.toBigInteger(value)

    override def toShort(value: java.math.BigInteger): Short =
      short.fromBigInteger(value)
    override def fromShort(value: Short): java.math.BigInteger =
      short.toBigInteger(value)

    override def toChar(value: java.math.BigInteger): Char =
      char.fromBigInteger(value)
    override def fromChar(value: Char): java.math.BigInteger =
      char.toBigInteger(value)

    override def toInt(value: java.math.BigInteger): Int =
      int.fromBigInteger(value)
    override def fromInt(value: Int): java.math.BigInteger =
      int.toBigInteger(value)

    override def toLong(value: java.math.BigInteger): Long =
      long.fromBigInteger(value)
    override def fromLong(value: Long): java.math.BigInteger =
      long.toBigInteger(value)

    override def toFloat(value: java.math.BigInteger): Float =
      float.fromBigInteger(value)
    override def fromFloat(value: Float): java.math.BigInteger =
      float.toBigInteger(value)

    override def toDouble(value: java.math.BigInteger): Double =
      double.fromBigInteger(value)
    override def fromDouble(value: Double): java.math.BigInteger =
      double.toBigInteger(value)

    override def toBigInteger(value: java.math.BigInteger): java.math.BigInteger =
      value
    override def fromBigInteger(value: java.math.BigInteger): java.math.BigInteger =
      value

    override def toBigDecimal(value: java.math.BigInteger): java.math.BigDecimal =
      new java.math.BigDecimal(value)
    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigInteger =
      value.toBigInteger
  }

  implicit val bigDecimal: TypeExtension[java.math.BigDecimal] = new TypeExtension[java.math.BigDecimal] {
    val tpe: ru.Type = ru.typeOf[java.math.BigDecimal].dealias

    override def toBytes(value: java.math.BigDecimal): Array[Byte] =
      value.unscaledValue().toByteArray

    override def fromBytes(value: Array[Byte]): java.math.BigDecimal =
      new java.math.BigDecimal(new java.math.BigInteger(1, value))

    override def toString(value: java.math.BigDecimal): String = value.toString

    override def fromString(value: String): java.math.BigDecimal =
      new java.math.BigDecimal(value)

    override def toBoolean(value: java.math.BigDecimal): Boolean =
      boolean.fromBigDecimal(value)

    override def fromBoolean(value: Boolean): java.math.BigDecimal =
      boolean.toBigDecimal(value)

    override def toByte(value: java.math.BigDecimal): Byte =
      byte.fromBigDecimal(value)

    override def fromByte(value: Byte): java.math.BigDecimal =
      byte.toBigDecimal(value)

    override def toShort(value: java.math.BigDecimal): Short =
      short.fromBigDecimal(value)

    override def fromShort(value: Short): java.math.BigDecimal =
      short.toBigDecimal(value)

    override def toChar(value: java.math.BigDecimal): Char =
      char.fromBigDecimal(value)

    override def fromChar(value: Char): java.math.BigDecimal =
      char.toBigDecimal(value)

    override def toInt(value: java.math.BigDecimal): Int =
      int.fromBigDecimal(value)

    override def fromInt(value: Int): java.math.BigDecimal =
      int.toBigDecimal(value)

    override def toLong(value: java.math.BigDecimal): Long =
      long.fromBigDecimal(value)

    override def fromLong(value: Long): java.math.BigDecimal =
      long.toBigDecimal(value)

    override def toFloat(value: java.math.BigDecimal): Float =
      float.fromBigDecimal(value)

    override def fromFloat(value: Float): java.math.BigDecimal =
      float.toBigDecimal(value)

    override def toDouble(value: java.math.BigDecimal): Double =
      double.fromBigDecimal(value)

    override def fromDouble(value: Double): java.math.BigDecimal =
      double.toBigDecimal(value)

    override def toBigInteger(value: java.math.BigDecimal): java.math.BigInteger =
      value.toBigInteger

    override def fromBigInteger(value: java.math.BigInteger): java.math.BigDecimal =
      new java.math.BigDecimal(value)

    override def toBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal =
      value

    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal =
      value
  }

  implicit val bytes: TypeExtension[Array[Byte]] = new TypeExtension[Array[Byte]] {
    val tpe: ru.Type = ru.typeOf[Array[Byte]].dealias

    override def toBytes(value: Array[Byte]): Array[Byte] = value
    override def fromBytes(value: Array[Byte]): Array[Byte] = value

    override def toString(value: Array[Byte]): String = new String(value, "UTF-8")
    override def fromString(value: String): Array[Byte] = value.getBytes("UTF-8")

    override def toBoolean(value: Array[Byte]): Boolean = boolean.fromBytes(value)
    override def fromBoolean(value: Boolean): Array[Byte] = boolean.toBytes(value)

    override def toByte(value: Array[Byte]): Byte = byte.fromBytes(value)
    override def fromByte(value: Byte): Array[Byte] = byte.toBytes(value)

    override def toShort(value: Array[Byte]): Short = short.fromBytes(value)
    override def fromShort(value: Short): Array[Byte] = short.toBytes(value)

    override def toChar(value: Array[Byte]): Char = char.fromBytes(value)
    override def fromChar(value: Char): Array[Byte] = char.toBytes(value)

    override def toInt(value: Array[Byte]): Int = int.fromBytes(value)
    override def fromInt(value: Int): Array[Byte] = int.toBytes(value)

    override def toLong(value: Array[Byte]): Long = long.fromBytes(value)
    override def fromLong(value: Long): Array[Byte] = long.toBytes(value)

    override def toFloat(value: Array[Byte]): Float = float.fromBytes(value)
    override def fromFloat(value: Float): Array[Byte] = float.toBytes(value)

    override def toDouble(value: Array[Byte]): Double = double.fromBytes(value)
    override def fromDouble(value: Double): Array[Byte] = double.toBytes(value)

    override def toBigInteger(value: Array[Byte]): java.math.BigInteger =
      bigInteger.fromBytes(value)
    override def fromBigInteger(value: java.math.BigInteger): Array[Byte] =
      bigInteger.toBytes(value)

    override def toBigDecimal(value: Array[Byte]): java.math.BigDecimal =
      bigDecimal.fromBytes(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Array[Byte] =
      bigDecimal.toBytes(value)
  }

  implicit val string: TypeExtension[String] = new TypeExtension[String] {
    val tpe: ru.Type = ru.typeOf[String].dealias

    override def toBytes(value: String): Array[Byte] = bytes.fromString(value)
    override def fromBytes(value: Array[Byte]): String = bytes.toString(value)

    override def toString(value: String): String = value
    override def fromString(value: String): String = value

    override def toBoolean(value: String): Boolean = boolean.fromString(value)
    override def fromBoolean(value: Boolean): String = boolean.toString(value)

    override def toByte(value: String): Byte = byte.fromString(value)
    override def fromByte(value: Byte): String = byte.toString(value)

    override def toShort(value: String): Short = short.fromString(value)
    override def fromShort(value: Short): String = short.toString(value)

    override def toChar(value: String): Char = char.fromString(value)
    override def fromChar(value: Char): String = char.toString(value)

    override def toInt(value: String): Int = int.fromString(value)
    override def fromInt(value: Int): String = int.toString(value)

    override def toLong(value: String): Long = long.fromString(value)
    override def fromLong(value: Long): String = long.toString(value)

    override def toFloat(value: String): Float = float.fromString(value)
    override def fromFloat(value: Float): String = float.toString(value)

    override def toDouble(value: String): Double = double.fromString(value)
    override def fromDouble(value: Double): String = double.toString(value)

    override def toBigInteger(value: String): java.math.BigInteger =
      bigInteger.fromString(value)
    override def fromBigInteger(value: java.math.BigInteger): String =
      bigInteger.toString(value)

    override def toBigDecimal(value: String): java.math.BigDecimal =
      bigDecimal.fromString(value)
    override def fromBigDecimal(value: java.math.BigDecimal): String =
      bigDecimal.toString(value)
  }
}
