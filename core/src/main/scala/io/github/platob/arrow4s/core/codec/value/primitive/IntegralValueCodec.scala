package io.github.platob.arrow4s.core.codec.value.primitive

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class IntegralValueCodec[T : ru.TypeTag : ClassTag] extends NumericValueCodec[T] {

}

object IntegralValueCodec {
  def fromArrowType(atype: ArrowType.Int): IntegralValueCodec[_] = {
    if (atype.getIsSigned) {
      atype.getBitWidth match {
        case 8  => ValueCodec.byte
        case 16 => ValueCodec.short
        case 32 => ValueCodec.int
        case 64 => ValueCodec.long
        case other => throw new IllegalArgumentException(s"Unsupported Int bit width: $other")
      }
    } else {
      atype.getBitWidth match {
        case 8  => ValueCodec.ubyte
        case 16 => ValueCodec.char
        case 32 => ValueCodec.uint
        case 64 => ValueCodec.ulong
        case other => throw new IllegalArgumentException(s"Unsupported UInt bit width: $other")
      }
    }
  }

  def fromArrowType(dec: ArrowType.Decimal): IntegralValueCodec[_] = {
    (dec.getScale, dec.getPrecision) match {
      case (scale, precision) if precision <= 38 && scale == 0 => ValueCodec.bigInteger
      case _ => throw new IllegalArgumentException(s"Unsupported decimal precision: ${dec.getPrecision}")
    }
  }

  class BooleanValueCodec extends IntegralValueCodec[Boolean] {
    override val bitSize: Int = 1
    override val arrowField: Field = ArrowField.build(
      name = "Boolean",
      at = ArrowType.Bool.INSTANCE,
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Boolean): Array[Byte] = Array(if (value) 1.toByte else 0.toByte)
    override def fromBytes(value: Array[Byte]): Boolean = {
      require(value.nonEmpty, s"Expected 1 byte or more, got ${value.length}")
      value(0) != ValueCodec.byte.zero
    }

    override def toString(value: Boolean, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Boolean = value.head match {
      case 't' | 'T' | '1' | 'y' | 'Y' => true
      case 'f' | 'F' | '0' | 'n' | 'N' => false
      case other => throw new IllegalArgumentException(s"Cannot parse Boolean from string starting with '$other'")
    }

    override def toBoolean(value: Boolean): Boolean = value
    override def fromBoolean(value: Boolean): Boolean = value

    override def toInt(value: Boolean): Int = if (value) 1 else 0
    override def fromInt(value: Int): Boolean = value != 0

    override def toLong(value: Boolean): Long = if (value) 1L else 0L
    override def fromLong(value: Long): Boolean = value != 0L

    override def toBigInteger(value: Boolean): java.math.BigInteger =
      if (value) java.math.BigInteger.ONE else java.math.BigInteger.ZERO
    override def fromBigInteger(value: java.math.BigInteger): Boolean =
      value != java.math.BigInteger.ZERO

    override def toDouble(value: Boolean): Double = if (value) 1.0 else 0.0
    override def fromDouble(value: Double): Boolean = value != 0.0

    override def toBigDecimal(value: Boolean): java.math.BigDecimal =
      if (value) java.math.BigDecimal.ONE else java.math.BigDecimal.ZERO
    override def fromBigDecimal(value: java.math.BigDecimal): Boolean =
      value != java.math.BigDecimal.ZERO
  }

  class ByteValueCodec extends IntegralValueCodec[Byte] {
    override val bitSize: Int = 8
    override val arrowField: Field = ArrowField.build(
      name = "Byte",
      at = new ArrowType.Int(8, true),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Byte): Array[Byte] = Array(value)
    override def fromBytes(value: Array[Byte]): Byte = {
      require(value.nonEmpty, s"Expected 1 byte or more, got ${value.length}")
      value(0)
    }

    override def toString(value: Byte, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Byte = value.toByte

    override def toByte(value: Byte): Byte = value
    override def fromByte(value: Byte): Byte = value

    override def toInt(value: Byte): Int = value.toInt
    override def fromInt(value: Int): Byte = value.toByte

    override def toLong(value: Byte): Long = value.toLong
    override def fromLong(value: Long): Byte = value.toByte

    override def toBigInteger(value: Byte): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Byte =
      value.byteValue()

    override def toDouble(value: Byte): Double = value.toDouble
    override def fromDouble(value: Double): Byte = value.toByte

    override def toBigDecimal(value: Byte): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Byte =
      value.byteValue()
  }

  class UByteValueCodec extends IntegralValueCodec[UByte] {
    override val bitSize: Int = 8
    override val arrowField: Field = ArrowField.build(
      name = "UByte",
      at = new ArrowType.Int(8, false),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: UByte): Array[Byte] = Array(value.toByte)
    override def fromBytes(value: Array[Byte]): UByte = {
      require(value.nonEmpty, s"Expected 1 byte or more, got ${value.length}")
      UByte.unsafe(value(0))
    }

    override def toString(value: UByte, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): UByte = UByte.trunc(value.toShort)

    override def toUByte(value: UByte): UByte = value
    override def fromUByte(value: UByte): UByte = value

    override def toInt(value: UByte): Int = value.toShort.toInt
    override def fromInt(value: Int): UByte = UByte.trunc(value)

    override def toLong(value: UByte): Long = value.toShort.toLong
    override def fromLong(value: Long): UByte = UByte.trunc(value)

    override def toBigInteger(value: UByte): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toInt)
    override def fromBigInteger(value: java.math.BigInteger): UByte =
      UByte.trunc(value.intValue())

    override def toDouble(value: UByte): Double = value.toShort.toDouble
    override def fromDouble(value: Double): UByte = UByte.trunc(value.toInt.toShort)

    override def toBigDecimal(value: UByte): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): UByte =
      UByte.trunc(value.intValue())
  }

  class ShortValueCodec extends IntegralValueCodec[Short] {
    override val bitSize: Int = 16
    override val arrowField: Field = ArrowField.build(
      name = "Short",
      at = new ArrowType.Int(16, true),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Short): Array[Byte] = Array(
      ((value >> 8) & 0xFF).toByte,
      (value & 0xFF).toByte
    )
    override def fromBytes(value: Array[Byte]): Short = {
      require(value.length >= 2, s"Expected 2 bytes or more, got ${value.length}")
      ((value(0) & 0xFF) << 8 | (value(1) & 0xFF)).toShort
    }

    override def toString(value: Short, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Short = value.toShort

    override def toInt(value: Short): Int = value.toInt
    override def fromInt(value: Int): Short = value.toShort

    override def toLong(value: Short): Long = value.toLong
    override def fromLong(value: Long): Short = value.toShort

    override def toBigInteger(value: Short): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Short =
      value.shortValue()

    override def toDouble(value: Short): Double = value.toDouble
    override def fromDouble(value: Double): Short = value.toShort

    override def toBigDecimal(value: Short): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Short =
      value.shortValue()
  }

  class CharValueCodec extends IntegralValueCodec[Char] {
    override val bitSize: Int = 16
    override val arrowField: Field = ArrowField.build(
      name = "Char",
      at = new ArrowType.Int(16, false),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Char): Array[Byte] =
      ValueCodec.short.toBytes(value.toShort)
    override def fromBytes(value: Array[Byte]): Char = {
      ValueCodec.short.fromBytes(value).toChar
    }

    override def toString(value: Char, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Char = ValueCodec.int.fromString(value, charset).toChar

    override def toInt(value: Char): Int = value.toInt
    override def fromInt(value: Int): Char = value.toChar

    override def toLong(value: Char): Long = value.toInt.toLong
    override def fromLong(value: Long): Char = value.toChar

    override def toBigInteger(value: Char): java.math.BigInteger =
      ValueCodec.int.toBigInteger(value.toInt)
    override def fromBigInteger(value: java.math.BigInteger): Char =
      ValueCodec.int.fromBigInteger(value).toChar

    override def toDouble(value: Char): Double = value.toInt.toDouble
    override def fromDouble(value: Double): Char = value.toChar

    override def toBigDecimal(value: Char): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Char =
      value.intValue().toChar
  }

  class IntValueCodec extends IntegralValueCodec[Int] {
    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = "Int",
      at = new ArrowType.Int(32, true),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Int): Array[Byte] = Array(
      ((value >> 24) & 0xFF).toByte,
      ((value >> 16) & 0xFF).toByte,
      ((value >> 8) & 0xFF).toByte,
      (value & 0xFF).toByte
    )
    override def fromBytes(value: Array[Byte]): Int = {
      require(value.length >= 4, s"Expected 4 bytes or more, got ${value.length}")
      (value(0) & 0xFF) << 24 | (value(1) & 0xFF) << 16 | (value(2) & 0xFF) << 8 | (value(3) & 0xFF)
    }

    override def toString(value: Int, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Int = value.toInt

    override def toInt(value: Int): Int = value
    override def fromInt(value: Int): Int = value

    override def toLong(value: Int): Long = value.toLong
    override def fromLong(value: Long): Int = value.toInt

    override def toBigInteger(value: Int): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Int =
      value.intValue()

    override def toDouble(value: Int): Double = value.toDouble
    override def fromDouble(value: Double): Int = value.toInt

    override def toBigDecimal(value: Int): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Int =
      value.intValue()
  }

  class UIntValueCodec extends IntegralValueCodec[UInt] {
    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = "UInt",
      at = new ArrowType.Int(32, false),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: UInt): Array[Byte] = {
      ValueCodec.int.toBytes(value.toInt)
    }
    override def fromBytes(value: Array[Byte]): UInt = {
      UInt.unsafe(ValueCodec.int.fromBytes(value))
    }

    override def toString(value: UInt, charset: Charset): String =
      value.toString
    override def fromString(value: String, charset: Charset): UInt =
      UInt.trunc(ValueCodec.int.fromString(value, charset))

    override def toInt(value: UInt): Int = value.toInt
    override def fromInt(value: Int): UInt = UInt.trunc(value)

    override def toLong(value: UInt): Long = value.toLong
    override def fromLong(value: Long): UInt = UInt.trunc(value)

    override def toBigInteger(value: UInt): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong & 0xFFFFFFFFL)
    override def fromBigInteger(value: java.math.BigInteger): UInt =
      UInt.trunc(value.intValue())

    override def toDouble(value: UInt): Double = value.toDouble
    override def fromDouble(value: Double): UInt = UInt.trunc(value.toLong)

    override def toBigDecimal(value: UInt): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): UInt =
      UInt.trunc(value.longValue())
  }

  class LongValueCodec extends IntegralValueCodec[Long] {
    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = "Long",
      at = new ArrowType.Int(64, true),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Long): Array[Byte] = Array(
      ((value >> 56) & 0xFF).toByte,
      ((value >> 48) & 0xFF).toByte,
      ((value >> 40) & 0xFF).toByte,
      ((value >> 32) & 0xFF).toByte,
      ((value >> 24) & 0xFF).toByte,
      ((value >> 16) & 0xFF).toByte,
      ((value >> 8) & 0xFF).toByte,
      (value & 0xFF).toByte
    )

    override def fromBytes(value: Array[Byte]): Long = {
      require(value.length >= 8, s"Expected 8 bytes or more, got ${value.length}")
      (value(0).toLong & 0xFF) << 56 |
        (value(1).toLong & 0xFF) << 48 |
        (value(2).toLong & 0xFF) << 40 |
        (value(3).toLong & 0xFF) << 32 |
        (value(4).toLong & 0xFF) << 24 |
        (value(5).toLong & 0xFF) << 16 |
        (value(6).toLong & 0xFF) << 8 |
        (value(7).toLong & 0xFF)
    }

    override def toString(value: Long, charset: Charset): String = value.toString

    override def fromString(value: String, charset: Charset): Long = value.toLong

    override def toInt(value: Long): Int = value.toInt

    override def fromInt(value: Int): Long = value.toLong

    override def toLong(value: Long): Long = value

    override def fromLong(value: Long): Long = value

    override def toBigInteger(value: Long): java.math.BigInteger =
      java.math.BigInteger.valueOf(value)

    override def fromBigInteger(value: java.math.BigInteger): Long =
      value.longValue()

    override def toDouble(value: Long): Double = value.toDouble

    override def fromDouble(value: Double): Long = value.toLong

    override def toBigDecimal(value: Long): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value)

    override def fromBigDecimal(value: java.math.BigDecimal): Long =
      value.longValue()
  }

  class ULongValueCodec extends IntegralValueCodec[ULong] {
    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = "ULong",
      at = new ArrowType.Int(64, false),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: ULong): Array[Byte] = {
      ValueCodec.long.toBytes(value.toLong)
    }
    override def fromBytes(value: Array[Byte]): ULong = {
      ULong.unsafe(ValueCodec.long.fromBytes(value))
    }

    override def toString(value: ULong, charset: Charset): String =
      value.toString
    override def fromString(value: String, charset: Charset): ULong =
      ULong.trunc(ValueCodec.long.fromString(value, charset))

    override def toInt(value: ULong): Int = value.toInt
    override def fromInt(value: Int): ULong = ULong.trunc(value)

    override def toLong(value: ULong): Long = value.toLong
    override def fromLong(value: Long): ULong = ULong.trunc(value)

    override def toBigInteger(value: ULong): java.math.BigInteger =
      java.math.BigInteger.valueOf(value.toLong).and(java.math.BigInteger.valueOf(0xFFFFFFFFFFFFFFFFL))
    override def fromBigInteger(value: java.math.BigInteger): ULong =
      ULong.trunc(value.longValue())

    override def toDouble(value: ULong): Double = value.toDouble
    override def fromDouble(value: Double): ULong = ULong.trunc(value.toLong)

    override def toBigDecimal(value: ULong): java.math.BigDecimal =
      java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): ULong =
      ULong.trunc(value.longValue())
  }

  class BigIntegerValueCodec extends IntegralValueCodec[java.math.BigInteger] {
    override val bitSize: Int = 128 // Arbitrary, as BigInteger can be of any size
    override val arrowField: Field = ArrowField.build(
      name = "BigInteger",
      at = new ArrowType.Decimal(38, 0, 128),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: java.math.BigInteger): Array[Byte] = value.toByteArray
    override def fromBytes(value: Array[Byte]): java.math.BigInteger = new java.math.BigInteger(value)

    override def toString(value: java.math.BigInteger, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): java.math.BigInteger = new java.math.BigInteger(value)

    override def toInt(value: java.math.BigInteger): Int = value.intValue()
    override def fromInt(value: Int): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)

    override def toLong(value: java.math.BigInteger): Long = value.longValue()
    override def fromLong(value: Long): java.math.BigInteger = java.math.BigInteger.valueOf(value)

    override def toBigInteger(value: java.math.BigInteger): java.math.BigInteger = value
    override def fromBigInteger(value: java.math.BigInteger): java.math.BigInteger = value

    override def toDouble(value: java.math.BigInteger): Double =
      value.doubleValue()
    override def fromDouble(value: Double): java.math.BigInteger =
      java.math.BigDecimal.valueOf(value).toBigInteger

    override def toBigDecimal(value: java.math.BigInteger): java.math.BigDecimal =
      new java.math.BigDecimal(value)
    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigInteger =
      value.toBigInteger
  }
}