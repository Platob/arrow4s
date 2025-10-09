package io.github.platob.arrow4s.core.codec.nested

import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.values.ValueConverter

import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class NestedCodec[T](
  val tpe: ru.Type,
  val typeTag: ru.TypeTag[T],
  val clsTag: ClassTag[T]
) extends ValueCodec[T] {
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
  override def fromBytes(value: Array[Byte]): T = fromValue(value)(ValueConverter.fromBytes(this))

  override def fromString(value: String, charset: Charset): T = fromValue(value)(ValueConverter.fromString(this))
  override def toString(value: T, charset: Charset): String = toValue(value)(ValueConverter.toString(this))

  override def toBoolean(value: T): Boolean = toValue(value)(ValueConverter.toBit(this))
  override def fromBoolean(value: Boolean): T = fromValue(value)(ValueConverter.fromBit(this))

  override def toInt(value: T): Int = toValue(value)(ValueConverter.toInt(this))
  override def fromInt(value: Int): T = fromValue(value)(ValueConverter.fromInt(this))

  override def toLong(value: T): Long = toValue(value)(ValueConverter.toLong(this))
  override def fromLong(value: Long): T = fromValue(value)(ValueConverter.fromLong(this))

  override def toDouble(value: T): Double = toValue(value)(ValueConverter.toDouble(this))
  override def fromDouble(value: Double): T = fromValue(value)(ValueConverter.fromDouble(this))

  override def toBigInteger(value: T): java.math.BigInteger = toValue(value)(ValueConverter.toBigInteger(this))
  override def fromBigInteger(value: java.math.BigInteger): T = fromValue(value)(ValueConverter.fromBigInteger(this))

  override def toBigDecimal(value: T): java.math.BigDecimal = toValue(value)(ValueConverter.toBigDecimal(this))
  override def fromBigDecimal(value: java.math.BigDecimal): T = fromValue(value)(ValueConverter.fromBigDecimal(this))
}