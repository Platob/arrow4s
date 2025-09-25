package io.github.platob.arrow4s.core.values

trait NumberValue[T] {
  @inline def toBoolean: Boolean
  @inline def toByte: Byte
  @inline def toShort: Short
  @inline def toInt: Int
  @inline def toLong: Long
  @inline def toFloat: Float
  @inline def toDouble: Double
  @inline def toBigInteger: java.math.BigInteger
  @inline def toBigInt: BigInt = BigInt(toBigInteger)
  @inline def toBigDecimal: java.math.BigDecimal
  @inline def toBigDec: BigDecimal = BigDecimal(toBigDecimal)

  def +(that: T): T

  def -(that: T): T

  def *(that: T): T

  def /(that: T): T

  def %(that: T): T

  def unary_- : T

  def compare(that: T): Int
}
