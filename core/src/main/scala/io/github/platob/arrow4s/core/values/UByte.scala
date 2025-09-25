package io.github.platob.arrow4s.core.values

final class UByte private (private val b: Byte) extends AnyVal {
  override def toString: String = toInt.toString

  @inline def toBytes: Array[Byte] = Array(b)
  @inline def toBoolean: Boolean = b != 0
  @inline def toByte: Byte = b
  @inline def toShort: Short = (b & 0xFF).toShort
  @inline def toInt: Int = b & 0xFF
  @inline def toLong: Long = (b & 0xFF).toLong
  @inline def toFloat: Float = (b & 0xFF).toFloat
  @inline def toDouble: Double = (b & 0xFF).toDouble
  @inline def toBigInteger: java.math.BigInteger = java.math.BigInteger.valueOf(b & 0xFFL)
  @inline def toBigDecimal: java.math.BigDecimal = java.math.BigDecimal.valueOf(b & 0xFFL)

  def +(that: UByte): UByte = UByte.trunc(this.toInt + that.toInt)
  def -(that: UByte): UByte = UByte.trunc(this.toInt - that.toInt)
  def *(that: UByte): UByte = UByte.trunc(this.toInt * that.toInt)
  def /(that: UByte): UByte = UByte.trunc(this.toInt / that.toInt)
  def %(that: UByte): UByte = UByte.trunc(this.toInt % that.toInt)
  def unary_- : UByte = UByte.trunc(-this.toInt)
  def compare(that: UByte): Int = Integer.compare(this.toInt, that.toInt)
}

object UByte {
  val MinValue: UByte = new UByte(0)
  val MaxValue: UByte = new UByte(-1) // 0xFF

  @inline def apply(str: String): UByte = str.toInt match {
    case i if 0 <= i && i <= 255 => new UByte(i.toByte)
    case i => throw new IllegalArgumentException(s"UByte out of range: $i")
  }

  @inline def apply(i: Int): UByte =
    if (0 <= i && i <= 255) new UByte(i.toByte)
    else throw new IllegalArgumentException(s"UByte out of range: $i")

  @inline def trunc(ushort: UShort): UByte = new UByte(ushort.toByte)
  @inline def trunc(i: Int): UByte = new UByte((i & 0xFF).toByte)
  @inline def trunc(i: Long): UByte = new UByte((i & 0xFF).toByte)
}
