
package io.github.platob.arrow4s.core.values

import java.math.BigInteger

final class ULong private (private val l: Long) extends AnyVal {
  override def toString: String = java.lang.Long.toUnsignedString(l)

  @inline def toBytes: Array[Byte] = {
    Array(
      ((l >>> 56) & 0xFF).toByte,
      ((l >>> 48) & 0xFF).toByte,
      ((l >>> 40) & 0xFF).toByte,
      ((l >>> 32) & 0xFF).toByte,
      ((l >>> 24) & 0xFF).toByte,
      ((l >>> 16) & 0xFF).toByte,
      ((l >>> 8) & 0xFF).toByte,
      (l & 0xFF).toByte
    )
  }
  @inline def toBoolean: Boolean = l != 0L
  @inline def toByte: Byte = l.toByte
  @inline def toShort: Short = l.toShort
  @inline def toInt: Int = l.toInt
  @inline def toLong: Long = l
  @inline def toFloat: Float = (l & 0xFFFFFFFFFFFFFFFFL).toFloat
  @inline def toDouble: Double = (l & 0xFFFFFFFFFFFFFFFFL).toDouble
  @inline def toBigInt: BigInt = BigInt(toBigInteger)
  @inline def toBigInteger: java.math.BigInteger =
    if (l >= 0) java.math.BigInteger.valueOf(l)
    else java.math.BigInteger.valueOf(l & Long.MaxValue).setBit(63)
  @inline def toBigDecimal: java.math.BigDecimal =
    new java.math.BigDecimal(toBigInteger)

  def +(that: ULong): ULong = ULong.trunc(this.toBigInt + that.toBigInt)
  def -(that: ULong): ULong = ULong.trunc(this.toBigInt - that.toBigInt)
  def *(that: ULong): ULong = ULong.trunc(this.toBigInt * that.toBigInt)
  def /(that: ULong): ULong = ULong.trunc(this.toBigInt / that.toBigInt)
  def %(that: ULong): ULong = ULong.trunc(this.toBigInt % that.toBigInt)
  def unary_- : ULong = ULong.trunc(this.toBigInteger.negate())
  def compare(that: ULong): Int = {
    val a = this.toBigInteger
    val b = that.toBigInteger
    a.compareTo(b)
  }
}

object ULong {
  val MinValue: ULong = new ULong(0L)
  val MaxValue: ULong = new ULong(-1L) // 0xFFFFFFFFFFFFFFFF
  val Zero: ULong = MinValue
  val One: ULong = new ULong(1L)

  private val Mask = new java.math.BigInteger("FFFFFFFFFFFFFFFF", 16)

  @inline def trunc(bi: BigInt): ULong = trunc(bi.bigInteger)
  @inline def trunc(bi: BigInteger): ULong = new ULong(bi.and(Mask).longValue)
}
