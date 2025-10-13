package io.github.platob.arrow4s.core.codec.value.nested

import io.github.platob.arrow4s.core.codec.convert.ValueConverter
import io.github.platob.arrow4s.core.codec.value.ValueCodec

import java.math.BigInteger
import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class NestedValueCodec[T](
  val tpe: ru.Type,
  val typeTag: ru.TypeTag[T],
  val clsTag: ClassTag[T]
) extends ValueCodec[T] {
  override def namespace: String = s"${clsTag.runtimeClass.getSimpleName}[${tpe.typeArgs.map(_.typeSymbol.name).mkString(",")}]"

  override def isPrimitive: Boolean = false

  lazy val bitSize: Int = {
    // if all fixed size, sum them up
    val sizes = children.map(_.bitSize)

    if (!isOption && sizes.forall(_ > 0)) sizes.sum
    else -1
  }

  override def zero: T = {
    val arr = children.map(_.zero).toArray

    fromElements(arr)
  }

  override def one: T = {
    val arr = children.map(_.one).toArray

    fromElements(arr)
  }

  override def default: T = zero

  override def toBytes(value: T): Array[Byte] = toValue(value)(ValueConverter.toBytes(this))
  override def fromBytes(value: Array[Byte]): T =
    throw new UnsupportedOperationException(s"Cannot convert bytes to $this")

  override def toString(value: T, charset: Charset): String = toValue(value)(ValueConverter.toString(this))
  override def fromString(value: String, charset: Charset): T =
    throw new UnsupportedOperationException(s"Cannot convert string to $this")

  override def toBoolean(value: T): Boolean = toValue(value)(ValueConverter.toBoolean(this))
  override def fromBoolean(value: Boolean): T =
    throw new UnsupportedOperationException(s"Cannot convert boolean to $this")

  override def toInt(value: T): Int = toValue(value)(ValueConverter.toInt(this))
  override def fromInt(value: Int): T =
    throw new UnsupportedOperationException(s"Cannot convert int to $this")

  override def toLong(value: T): Long = toValue(value)(ValueConverter.toLong(this))
  override def fromLong(value: Long): T =
    throw new UnsupportedOperationException(s"Cannot convert long to $this")

  override def toDouble(value: T): Double = toValue(value)(ValueConverter.toDouble(this))
  override def fromDouble(value: Double): T =
    throw new UnsupportedOperationException(s"Cannot convert double to $this")

  override def toBigInteger(value: T): BigInteger = toValue(value)(ValueConverter.toBigInteger(this))
  override def fromBigInteger(value: BigInteger): T =
    throw new UnsupportedOperationException(s"Cannot convert BigInteger to $this")

  override def toBigDecimal(value: T): java.math.BigDecimal = toValue(value)(ValueConverter.toBigDecimal(this))
  override def fromBigDecimal(value: java.math.BigDecimal): T =
    throw new UnsupportedOperationException(s"Cannot convert BigDecimal to $this")
}