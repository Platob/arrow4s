package io.github.platob.arrow4s.core.codec

import io.github.platob.arrow4s.core.codec.ValueCodec.{bit, byte, bytes}
import io.github.platob.arrow4s.core.types.ArrowField
import io.github.platob.arrow4s.core.values.{UByte, ULong}

import java.nio.charset.Charset
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class OptionalCodec[T](
  inner: ValueCodec[T],
  tpe: ru.Type,
  typeTag: ru.TypeTag[Option[T]],
  clsTag: ClassTag[Option[T]],
) extends ProxyCodec[T, Option[T]](
  arrowField = ArrowField.javaBuild(
    name = inner.arrowField.getName,
    at = inner.arrowField.getType,
    nullable = true,
    metadata = inner.arrowField.getMetadata,
    children = inner.arrowField.getChildren
  ),
  inner = inner, tpe = tpe, typeTag = typeTag, clsTag = clsTag
) {
  override val bitSize = -1 // Variable size
  def encode(value: Option[T]): T = value.getOrElse(inner.default)
  def decode(value: T): Option[T] = if (value == inner.default) None else Some(value)

  override def elementAt[E](value: Option[T], index: Int): E = value match {
    case Some(v) => v.asInstanceOf[E]
    case None    => inner.default.asInstanceOf[E]
  }

  override def elements(value: Option[T]): Array[Any] = value match {
    case Some(v) => inner.elements(v)
    case None    => Array.empty
  }

  override def fromElements(values: Array[Any]): Option[T] = {
    if (values.isEmpty) None
    else Some(inner.fromElements(values))
  }

  override def toBytes(value: Option[T]): Array[Byte] = value match {
    case Some(v) => inner.toBytes(v)
    case None    => bytes.default
  }
  override def fromBytes(value: Array[Byte]): Option[T] = {
    if (value.isEmpty) None
    else Some(inner.fromBytes(value))
  }

  override def toString(value: Option[T], charset: Charset): String = value match {
    case Some(v) => inner.toString(v)
    case None    => ""
  }
  override def fromString(value: String, charset: Charset): Option[T] = {
    if (value.isEmpty || value.equalsIgnoreCase("null")) None
    else Some(inner.fromString(value))
  }

  override def toBoolean(value: Option[T]): Boolean = value match {
    case Some(v) => inner.toBoolean(v)
    case None    => bit.zero
  }
  override def fromBoolean(value: Boolean): Option[T] = Some(inner.fromBoolean(value))

  override def toByte(value: Option[T]): Byte = value match {
    case Some(v) => inner.toByte(v)
    case None    => byte.zero
  }
  override def fromByte(value: Byte): Option[T] = Some(inner.fromByte(value))

  override def toUByte(value: Option[T]): UByte = value match {
    case Some(v) => inner.toUByte(v)
    case None    => UByte.Zero
  }
  override def fromUByte(value: UByte): Option[T] = Some(inner.fromUByte(value))

  override def toShort(value: Option[T]): Short = value match {
    case Some(v) => inner.toShort(v)
    case None    => 0.toShort
  }
  override def fromShort(value: Short): Option[T] = Some(inner.fromShort(value))

  override def toChar(value: Option[T]): Char = value match {
    case Some(v) => inner.toChar(v)
    case None    => 0.toChar
  }
  override def fromChar(value: Char): Option[T] = Some(inner.fromChar(value))

  override def toInt(value: Option[T]): Int = value match {
    case Some(v) => inner.toInt(v)
    case None    => 0
  }
  override def fromInt(value: Int): Option[T] = Some(inner.fromInt(value))

  override def toLong(value: Option[T]): Long = value match {
    case Some(v) => inner.toLong(v)
    case None    => 0L
  }
  override def fromLong(value: Long): Option[T] = Some(inner.fromLong(value))

  override def toULong(value: Option[T]): ULong = value match {
    case Some(v) => inner.toULong(v)
    case None    => ULong.Zero
  }
  override def fromULong(value: ULong): Option[T] = Some(inner.fromULong(value))

  override def toBigInteger(value: Option[T]): java.math.BigInteger = value match {
    case Some(v) => inner.toBigInteger(v)
    case None    => java.math.BigInteger.ZERO
  }
  override def fromBigInteger(value: java.math.BigInteger): Option[T] = Some(inner.fromBigInteger(value))

  override def toFloat(value: Option[T]): Float = value match {
    case Some(v) => inner.toFloat(v)
    case None    => 0.0f
  }
  override def fromFloat(value: Float): Option[T] = Some(inner.fromFloat(value))

  override def toDouble(value: Option[T]): Double = value match {
    case Some(v) => inner.toDouble(v)
    case None    => 0.0
  }
  override def fromDouble(value: Double): Option[T] = Some(inner.fromDouble(value))

  override def toBigDecimal(value: Option[T]): java.math.BigDecimal = value match {
    case Some(v) => inner.toBigDecimal(v)
    case None    => java.math.BigDecimal.ZERO
  }
  override def fromBigDecimal(value: java.math.BigDecimal): Option[T] = Some(inner.fromBigDecimal(value))
}

object OptionalCodec {
  def make[T](implicit vc: ValueCodec[T]): OptionalCodec[T] = {
    implicit val baseTT: ru.TypeTag[T] = vc.typeTag
    implicit val baseCT: ClassTag[T] = vc.clsTag

    val tt = ru.typeTag[Option[T]]
    val tpe = tt.tpe
    val ct = classTag[Option[T]]

    new OptionalCodec[T](
      tpe = tpe,
      typeTag = tt,
      clsTag = ct,
      inner = vc
    )
  }
}
