
package io.github.platob.arrow4s.core.values

final class UShort private (private val s: Short) extends AnyVal {
  override def toString: String = toInt.toString

  @inline def toBytes: Array[Byte] = Array((s & 0xFF).toByte, ((s >> 8) & 0xFF).toByte)
  @inline def toBoolean: Boolean = s != 0
  @inline def toByte: Byte = (s & 0xFF).toByte
  @inline def toShort: Short = (s & 0xFFFF).toShort
  @inline def toInt: Int = s & 0xFFFF
  @inline def toLong: Long = s & 0xFFFFL
  @inline def toFloat: Float = (s & 0xFFFFL).toFloat
  @inline def toDouble: Double = (s & 0xFFFFL).toDouble
  @inline def toBigInteger: java.math.BigInteger = java.math.BigInteger.valueOf(s & 0xFFFFL)
  @inline def toBigDecimal: java.math.BigDecimal = java.math.BigDecimal.valueOf(s & 0xFFFFL)

  def +(that: UShort): UShort = UShort.trunc(this.toInt + that.toInt)
  def -(that: UShort): UShort = UShort.trunc(this.toInt - that.toInt)
  def *(that: UShort): UShort = UShort.trunc(this.toInt * that.toInt)
  def /(that: UShort): UShort = UShort.trunc(this.toInt / that.toInt)
  def %(that: UShort): UShort = UShort.trunc(this.toInt % that.toInt)
  def unary_- : UShort = UShort.trunc(-this.toInt)
  def compare(that: UShort): Int = Integer.compare(this.toInt, that.toInt)
}

object UShort {
  val MinValue: UShort = new UShort(0)
  val MaxValue: UShort = new UShort(-1) // 0xFFFF

  @inline def apply(str: String): UShort = apply(str.toInt)

  @inline def apply(i: Int): UShort =
    if (0 <= i && i <= 65535) new UShort(i.toShort)
    else throw new IllegalArgumentException(s"UShort out of range: $i")

  @inline def from(ubyte: UByte): UShort = new UShort(ubyte.toShort)
  @inline def trunc(i: Int): UShort = new UShort((i & 0xFFFF).toShort)
  @inline def trunc(i: Long): UShort = new UShort((i & 0xFFFF).toShort)
  @inline def fromChar(c: Char): UShort = new UShort(c.toShort)
}
