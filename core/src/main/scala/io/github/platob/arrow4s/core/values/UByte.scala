package io.github.platob.arrow4s.core.values

final class UByte private (private val b: Byte) extends AnyVal {
  override def toString: String = toInt.toString

  @inline def toBoolean: Boolean = b != 0
  @inline def toByte: Byte = b
  @inline def toShort: Short = (b & 0xFF).toShort
  @inline def toInt: Int = b & 0xFF
  @inline def toLong: Long = (b & 0xFF).toLong
  @inline def toFloat: Float = (b & 0xFF).toFloat
  @inline def toDouble: Double = (b & 0xFF).toDouble

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
  val One: UByte = new UByte(1)
  val Zero: UByte = MinValue

  @inline def unsafe(byte: Byte): UByte = new UByte(byte)
  @inline def trunc(byte: Byte): UByte = trunc(byte & 0xFF)
  @inline def trunc(short: Short): UByte = new UByte((short & 0xFF).toByte)
  @inline def trunc(i: Int): UByte = new UByte((i & 0xFF).toByte)
  @inline def trunc(i: Long): UByte = new UByte((i & 0xFF).toByte)
}
