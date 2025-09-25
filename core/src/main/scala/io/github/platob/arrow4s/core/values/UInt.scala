
package io.github.platob.arrow4s.core.values

final class UInt private (private val i: Int) extends AnyVal {
  override def toString: String = java.lang.Integer.toUnsignedString(i)

  @inline def toBytes: Array[Byte] = Array(
    ((i >>> 24) & 0xFF).toByte,
    ((i >>> 16) & 0xFF).toByte,
    ((i >>> 8) & 0xFF).toByte,
    (i & 0xFF).toByte
  )
  @inline def toBoolean: Boolean = i != 0
  @inline def toByte: Byte = i.toByte
  @inline def toShort: Short = i.toShort
  @inline def toInt: Int = i
  @inline def toLong: Long = i & 0xFFFFFFFFL
  @inline def toFloat: Float = (i & 0xFFFFFFFFL).toFloat
  @inline def toDouble: Double = (i & 0xFFFFFFFFL).toDouble
  @inline def toBigInteger: java.math.BigInteger =
    java.math.BigInteger.valueOf(i & 0xFFFFFFFFL)
  @inline def toBigDecimal: java.math.BigDecimal =
    java.math.BigDecimal.valueOf(i & 0xFFFFFFFFL)

  def +(that: UInt): UInt = UInt.trunc(this.toLong + that.toLong)
  def -(that: UInt): UInt = UInt.trunc(this.toLong - that.toLong)
  def *(that: UInt): UInt = UInt.trunc(this.toLong * that.toLong)
  def /(that: UInt): UInt = UInt.trunc(java.lang.Long.divideUnsigned(this.toLong, that.toLong))
  def %(that: UInt): UInt = UInt.trunc(java.lang.Long.remainderUnsigned(this.toLong, that.toLong))
  def unary_- : UInt = UInt.trunc(-this.toLong)
  def compare(that: UInt): Int = java.lang.Long.compareUnsigned(this.toLong, that.toLong)
}

object UInt {
  val MinValue: UInt = new UInt(0)
  val MaxValue: UInt = new UInt(-1) // 0xFFFFFFFF

  @inline def trunc(str: String): UInt = trunc(str.toLong)
  @inline def trunc(i: Int): UInt = trunc(i & 0xFFFFFFFFL)
  @inline def trunc(l: Long): UInt = new UInt((l & 0xFFFFFFFF).toInt)
}
