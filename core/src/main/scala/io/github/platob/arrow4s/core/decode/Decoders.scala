package io.github.platob.arrow4s.core.decode

import org.apache.arrow.vector._

import java.nio.charset.Charset

object Decoders {
  implicit val intDecoder: Decoder.Typed[Int, IntVector] = new Decoder.Typed[Int, IntVector] {
    val isOptional: Boolean = false

    override def get(vector: IntVector, index: Int): Int = vector.get(index)
  }

  implicit val uintDecoder: Decoder.Typed[Int, UInt4Vector] = new Decoder.Typed[Int, UInt4Vector] {
    val isOptional: Boolean = false

    override def get(vector: UInt4Vector, index: Int): Int = vector.get(index)
  }

  implicit val longDecoder: Decoder.Typed[Long, BigIntVector] = new Decoder.Typed[Long, BigIntVector] {
    val isOptional: Boolean = false

    override def get(vector: BigIntVector, index: Int): Long = vector.get(index)
  }

  implicit val ulongDecoder: Decoder.Typed[Long, UInt8Vector] = new Decoder.Typed[Long, UInt8Vector] {
    val isOptional: Boolean = false

    override def get(vector: UInt8Vector, index: Int): Long = vector.get(index)
  }

  implicit val stringDecoder: Decoder.Typed[String, VarCharVector] = new Decoder.Typed[String, VarCharVector] {
    val isOptional: Boolean = false

    val charset: Charset = Charset.forName("UTF-8")

    override def get(vector: VarCharVector, index: Int): String = {
      val bytes = vector.get(index)
      new String(bytes, charset)
    }
  }

  implicit val booleanDecoder: Decoder.Typed[Boolean, BitVector] = new Decoder.Typed[Boolean, BitVector] {
    val isOptional: Boolean = false

    override def get(vector: BitVector, index: Int): Boolean = vector.get(index) == 1
  }

  implicit val doubleDecoder: Decoder.Typed[Double, Float8Vector] = new Decoder.Typed[Double, Float8Vector] {
    val isOptional: Boolean = false

    override def get(vector: Float8Vector, index: Int): Double = vector.get(index)
  }

  implicit val floatDecoder: Decoder.Typed[Float, Float4Vector] = new Decoder.Typed[Float, Float4Vector] {
    val isOptional: Boolean = false

    override def get(vector: Float4Vector, index: Int): Float = vector.get(index)
  }

  implicit val shortDecoder: Decoder.Typed[Short, SmallIntVector] = new Decoder.Typed[Short, SmallIntVector] {
    val isOptional: Boolean = false

    override def get(vector: SmallIntVector, index: Int): Short = vector.get(index)
  }

  implicit val ushortDecoder: Decoder.Typed[Char, UInt2Vector] = new Decoder.Typed[Char, UInt2Vector] {
    val isOptional: Boolean = false

    override def get(vector: UInt2Vector, index: Int): Char = vector.get(index)
  }

  implicit val byteDecoder: Decoder.Typed[Byte, UInt1Vector] = new Decoder.Typed[Byte, UInt1Vector] {
    val isOptional: Boolean = false

    override def get(vector: UInt1Vector, index: Int): Byte = vector.get(index)
  }
}
