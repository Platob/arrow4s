package io.github.platob.arrow4s.core.codec.value.primitive

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.math.BigInteger
import java.nio.charset.Charset
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class FloatingValueCodec[T : ru.TypeTag : ClassTag] extends NumericValueCodec[T] {

}

object FloatingValueCodec {
  def fromArrowType(f: ArrowType.FloatingPoint): FloatingValueCodec[_] = {
    f.getPrecision match {
      case FloatingPointPrecision.SINGLE => ValueCodec.float
      case FloatingPointPrecision.DOUBLE => ValueCodec.double
      case _ => throw new IllegalArgumentException(s"Unsupported floating point precision: ${f.getPrecision}")
    }
  }

  def fromArrowType(dec: ArrowType.Decimal): FloatingValueCodec[_] = {
    (dec.getScale, dec.getPrecision) match {
      case (scale, precision) if precision <= 38 => ValueCodec.bigDecimal
      case _ => throw new IllegalArgumentException(s"Unsupported decimal precision: ${dec.getPrecision}")
    }
  }

  class FloatValueCodec extends FloatingValueCodec[Float] {
    override def namespace: String = "Float"

    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = namespace,
      at = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Float): Array[Byte] = {
      val intBits = java.lang.Float.floatToIntBits(value)
      ValueCodec.int.toBytes(intBits)
    }

    override def fromBytes(bytes: Array[Byte]): Float = {
      val intBits = ValueCodec.int.fromBytes(bytes)
      java.lang.Float.intBitsToFloat(intBits)
    }

    override def toString(value: Float, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Float = value.toFloat

    override def toInt(value: Float): Int = value.toInt
    override def fromInt(value: Int): Float = value.toFloat

    override def toLong(value: Float): Long = value.toLong
    override def fromLong(value: Long): Float = value.toFloat

    override def toFloat(value: Float): Float = value
    override def fromFloat(value: Float): Float = value

    override def toDouble(value: Float): Double = value.toDouble
    override def fromDouble(value: Double): Float = value.toFloat

    override def toBigInteger(value: Float): java.math.BigInteger = BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Float = value.floatValue()

    override def toBigDecimal(value: Float): java.math.BigDecimal = new java.math.BigDecimal(value.toDouble)
    override def fromBigDecimal(value: java.math.BigDecimal): Float = value.floatValue()
  }

  class DoubleValueCodec extends FloatingValueCodec[Double] {
    override def namespace: String = "Double"

    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = namespace,
      at = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Double): Array[Byte] = {
      val longBits = java.lang.Double.doubleToLongBits(value)
      ValueCodec.long.toBytes(longBits)
    }

    override def fromBytes(bytes: Array[Byte]): Double = {
      val longBits = ValueCodec.long.fromBytes(bytes)
      java.lang.Double.longBitsToDouble(longBits)
    }

    override def toString(value: Double, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Double = value.toDouble

    override def toInt(value: Double): Int = value.toInt
    override def fromInt(value: Int): Double = value.toDouble

    override def toLong(value: Double): Long = value.toLong
    override def fromLong(value: Long): Double = value.toDouble

    override def toFloat(value: Double): Float = value.toFloat
    override def fromFloat(value: Float): Double = value.toDouble

    override def toDouble(value: Double): Double = value
    override def fromDouble(value: Double): Double = value

    override def toBigInteger(value: Double): java.math.BigInteger = BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Double = value.doubleValue()

    override def toBigDecimal(value: Double): java.math.BigDecimal = new java.math.BigDecimal(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Double = value.doubleValue()
  }

  class BigDecimalValueCodec extends FloatingValueCodec[java.math.BigDecimal] {
    override def namespace: String = "BigDecimal"

    override val bitSize: Int = 128 // Arbitrary precision, but we can define a typical size
    override val arrowField: Field = ArrowField.build(
      name = namespace,
      at = new ArrowType.Decimal(38, 18, 128), // No direct BigDecimal type in Arrow
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: java.math.BigDecimal): Array[Byte] = {
      value.unscaledValue().toByteArray
    }

    override def fromBytes(bytes: Array[Byte]): java.math.BigDecimal = {
      val bigInt = new BigInteger(bytes)
      new java.math.BigDecimal(bigInt)
    }

    override def toString(value: java.math.BigDecimal, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toInt(value: java.math.BigDecimal): Int = value.intValue()
    override def fromInt(value: Int): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toLong(value: java.math.BigDecimal): Long = value.longValue()
    override def fromLong(value: Long): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toFloat(value: java.math.BigDecimal): Float = value.floatValue()
    override def fromFloat(value: Float): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toDouble(value: java.math.BigDecimal): Double = value.doubleValue()
    override def fromDouble(value: Double): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toBigInteger(value: java.math.BigDecimal): java.math.BigInteger = value.toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal = value
    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal = value
  }
}
