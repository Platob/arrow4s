package io.github.platob.arrow4s.core.codec

import org.apache.arrow.vector.types.pojo.Field

import java.math.BigInteger
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class ProxyCodec[In, Out](
  val arrowField: Field,
  val inner: ValueCodec[In],
  val tpe: ru.Type,
  val typeTag: ru.TypeTag[Out],
  val clsTag: ClassTag[Out],
) extends ValueCodec[Out] {
  def encode(value: Out): In
  def decode(value: In): Out

  override def namespace: String = tpe.typeSymbol.fullName
  override def one: Out = decode(inner.one)
  override def zero: Out = decode(inner.zero)
  override def default: Out = decode(inner.default)
  override def bitSize: Int = inner.bitSize

  override def children: Seq[ValueCodec[_]] = Seq(inner)

  override def elementAt[E](value: Out, index: Int): E = inner.elementAt[E](encode(value), index)

  override def elements(value: Out): Array[Any] = inner.elements(encode(value))

  override def fromElements(values: Array[Any]): Out = decode(inner.fromElements(values))

  override def toBytes(value: Out): Array[Byte] = inner.toBytes(encode(value))
  override def fromBytes(value: Array[Byte]): Out = decode(inner.fromBytes(value))

  override def toString(value: Out, charset: java.nio.charset.Charset): String = inner.toString(encode(value), charset)
  override def fromString(value: String, charset: java.nio.charset.Charset): Out = decode(inner.fromString(value, charset))

  override def toBoolean(value: Out): Boolean = inner.toBoolean(encode(value))
  override def fromBoolean(value: Boolean): Out = decode(inner.fromBoolean(value))

  override def toByte(value: Out): Byte = inner.toByte(encode(value))
  override def fromByte(value: Byte): Out = decode(inner.fromByte(value))

  override def toShort(value: Out): Short = inner.toShort(encode(value))
  override def fromShort(value: Short): Out = decode(inner.fromShort(value))

  override def toChar(value: Out): Char = inner.toChar(encode(value))
  override def fromChar(value: Char): Out = decode(inner.fromChar(value))

  override def toInt(value: Out): Int = inner.toInt(encode(value))
  override def fromInt(value: Int): Out = decode(inner.fromInt(value))

  override def toLong(value: Out): Long = inner.toLong(encode(value))
  override def fromLong(value: Long): Out = decode(inner.fromLong(value))

  override def toFloat(value: Out): Float = inner.toFloat(encode(value))
  override def fromFloat(value: Float): Out = decode(inner.fromFloat(value))

  override def toDouble(value: Out): Double = inner.toDouble(encode(value))
  override def fromDouble(value: Double): Out = decode(inner.fromDouble(value))

  override def toBigInteger(value: Out): BigInteger = inner.toBigInteger(encode(value))
  override def fromBigInteger(value: BigInteger): Out = decode(inner.fromBigInteger(value))

  override def toBigDecimal(value: Out): java.math.BigDecimal = inner.toBigDecimal(encode(value))
  override def fromBigDecimal(value: java.math.BigDecimal): Out = decode(inner.fromBigDecimal(value))
}
