package io.github.platob.arrow4s.core.cast

import org.apache.arrow.vector.FieldVector

trait NumericEncoder[T] extends PrimitiveEncoder[T] {
  val maxValue: T
  val minValue: T

  override def toBoolean(value: T): Boolean = if (value == zero) false else true
  override def fromBoolean(value: Boolean): T = if (value) one else zero

  /**
   * Default numeric parse EPOCH seconds.nanos to java.time.Instant
   * @param value value in seconds.nanos
   * @return
   */
  override def toInstant(value: T): java.time.Instant = {
    val bd = toBigDecimal(value)
    val seconds = bd.setScale(0, java.math.RoundingMode.DOWN).longValueExact()
    val nanos = bd.subtract(java.math.BigDecimal.valueOf(seconds))
      .multiply(java.math.BigDecimal.valueOf(1e9))
      .longValueExact()

    java.time.Instant.ofEpochSecond(seconds, nanos)
  }
  override def fromInstant(value: java.time.Instant): T = {
    val seconds = java.math.BigDecimal.valueOf(value.getEpochSecond)
    val nanos = java.math.BigDecimal.valueOf(value.getNano).divide(java.math.BigDecimal.valueOf(1e9))

    fromBigDecimal(seconds.add(nanos))
  }
}

object NumericEncoder {
  trait Arrow[T, V <: FieldVector] extends NumericEncoder[T] with PrimitiveEncoder.Arrow[T, V] {

  }
}
