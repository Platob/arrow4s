package io.github.platob.arrow4s.core.cast

import org.apache.arrow.vector.FieldVector

trait LogicalEncoder[L, T] extends AnyEncoder[L] {
  @inline def underlying: AnyEncoder[T]
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
}

object LogicalEncoder {
  trait Arrow[L, T, V <: FieldVector] extends LogicalEncoder[L, T] with AnyEncoder.Arrow[L, V] {
    override def underlying: AnyEncoder.Arrow[T, V]

    // Mutator for Arrow Vectors
    override def setVector(vector: V, index: Int, value: L): Unit =
      underlying.setVector(vector, index, fromLogical(value))

    // Accessor for Arrow Vectors
    override def getVector(vector: V, index: Int): L =
      toLogical(underlying.getVector(vector, index))
  }

  def logical[L, T](to: T => L, from: L => T)(implicit base: AnyEncoder[T]): LogicalEncoder[L, T] =
    new LogicalEncoder[L, T] {
      override def underlying: AnyEncoder[T] = base
      override def toLogical(value: T): L = to(value)
      override def fromLogical(value: L): T = from(value)
    }

  def logicalArrow[L, T, V <: FieldVector](to: T => L, from: L => T)(implicit base: AnyEncoder.Arrow[T, V]): LogicalEncoder.Arrow[L, T, V] =
    new LogicalEncoder.Arrow[L, T, V] {
      override def underlying: AnyEncoder.Arrow[T, V] = base
      override def toLogical(value: T): L = to(value)
      override def fromLogical(value: L): T = from(value)

      // Mutator for Arrow Vectors
      override def setVector(vector: V, index: Int, value: L): Unit =
        base.setVector(vector, index, fromLogical(value))

      // Accessor for Arrow Vectors
      override def getVector(vector: V, index: Int): L =
        toLogical(base.getVector(vector, index))
    }

  def optionalArrow[T, V <: FieldVector](implicit base: AnyEncoder.Arrow[T, V]): LogicalEncoder.Arrow[Option[T], T, V] =
    new LogicalEncoder.Arrow[Option[T], T, V] {
      override def underlying: AnyEncoder.Arrow[T, V] = base
      override def toLogical(value: T): Option[T] = Option(value)
      override def fromLogical(value: Option[T]): T = value.getOrElse(base.zero)

      // Mutator for Arrow Vectors
      override def setVector(vector: V, index: Int, value: Option[T]): Unit = {
        base.setVectorOption(vector, index, value)
      }

      // Accessor for Arrow Vectors
      override def getVector(vector: V, index: Int): Option[T] = {
        base.getVectorOption(vector, index)
      }
    }
}
