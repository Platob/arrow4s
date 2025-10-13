package io.github.platob.arrow4s.core.codec.value.primitive

import io.github.platob.arrow4s.core.codec.value.{ReflectedCodec, ValueCodec}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}

import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class PrimitiveValueCodec[T : ru.TypeTag : ClassTag] extends ReflectedCodec[T] {
  def default: T = zero

  override def isPrimitive: Boolean = true

  override def isOption: Boolean = false

  override def isTuple: Boolean = false

  override def elementAt[E](value: T, index: Int): E =
    if (index == 0) value.asInstanceOf[E]
    else throw new IndexOutOfBoundsException(s"Primitive type $tpe has no element at index $index")

  override def elements(value: T): Array[Any] = Array(value)

  override def fromElements(values: Array[Any]): T =
    if (values.isEmpty) default
    else unsafeAs(values(0))

  override def children: Seq[ValueCodec[_]] = Seq.empty

  // Convenience common types
  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toString(value: T): String = toString(value, ByteBasedValueCodec.UTF8_CHARSET)
  @inline def fromString(value: String): T = fromString(value, ByteBasedValueCodec.UTF8_CHARSET)

  @inline def toString(value: T, charset: Charset): String
  @inline def fromString(value: String, charset: Charset): T

  @inline def toBoolean(value: T): Boolean
  @inline def fromBoolean(value: Boolean): T

  @inline def toByte(value: T): Byte = toInt(value).toByte
  @inline def fromByte(value: Byte): T = fromInt(value.toInt)

  @inline def toUByte(value: T): UByte = UByte.trunc(toInt(value))
  @inline def fromUByte(value: UByte): T = fromInt(value.toInt)

  @inline def toShort(value: T): Short = toInt(value).toShort
  @inline def fromShort(value: Short): T = fromInt(value.toInt)

  @inline def toChar(value: T): Char = toInt(value).toChar
  @inline def fromChar(value: Char): T = fromInt(value.toInt)

  @inline def toInt(value: T): Int
  @inline def fromInt(value: Int): T

  @inline def toUInt(value: T): UInt = UInt.trunc(toLong(value))
  @inline def fromUInt(value: UInt): T = fromLong(value.toInt)

  @inline def toLong(value: T): Long
  @inline def fromLong(value: Long): T

  @inline def toULong(value: T): ULong = ULong.trunc(toBigInteger(value))
  @inline def fromULong(value: ULong): T = fromBigInteger(value.toBigInteger)

  @inline def toBigInteger(value: T): java.math.BigInteger
  @inline def fromBigInteger(value: java.math.BigInteger): T

  @inline def toFloat(value: T): Float = toDouble(value).toFloat
  @inline def fromFloat(value: Float): T = fromDouble(value.toDouble)

  @inline def toDouble(value: T): Double
  @inline def fromDouble(value: Double): T

  @inline def toBigDecimal(value: T): java.math.BigDecimal
  @inline def fromBigDecimal(value: java.math.BigDecimal): T
}
