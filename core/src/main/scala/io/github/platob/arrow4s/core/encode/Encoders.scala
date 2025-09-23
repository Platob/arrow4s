package io.github.platob.arrow4s.core.encode

import org.apache.arrow.vector._

import java.nio.charset.Charset

object Encoders {
  implicit val intEncoder: Encoder.Typed[Int, IntVector] = {
    new Encoder.Typed[Int, IntVector] {
      override def set(vector: IntVector, index: Int, value: Int): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val uintEncoder: Encoder.Typed[Int, UInt4Vector] = {
    new Encoder.Typed[Int, UInt4Vector] {
      override def set(vector: UInt4Vector, index: Int, value: Int): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val longEncoder: Encoder.Typed[Long, BigIntVector] = {
    new Encoder.Typed[Long, BigIntVector] {
      override def set(vector: BigIntVector, index: Int, value: Long): Unit = {
        vector.set(index, value.toInt)
      }
    }
  }

  implicit val ulongEncoder: Encoder.Typed[Long, UInt8Vector] = {
    new Encoder.Typed[Long, UInt8Vector] {
      override def set(vector: UInt8Vector, index: Int, value: Long): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val stringEncoder: Encoder.Typed[String, VarCharVector] = {
    new Encoder.Typed[String, VarCharVector] {
      val charset: Charset = Charset.forName("UTF-8")

      override def set(vector: VarCharVector, index: Int, value: String): Unit = {
        vector.setSafe(index, value.getBytes(charset))
      }
    }
  }

  implicit val booleanEncoder: Encoder.Typed[Boolean, BitVector] = {
    new Encoder.Typed[Boolean, BitVector] {
      override def set(vector: BitVector, index: Int, value: Boolean): Unit = {
        vector.set(index, if (value) 1 else 0)
      }
    }
  }

  implicit val doubleEncoder: Encoder.Typed[Double, Float8Vector] = {
    new Encoder.Typed[Double, Float8Vector] {
      override def set(vector: Float8Vector, index: Int, value: Double): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val floatEncoder: Encoder.Typed[Float, Float4Vector] = {
    new Encoder.Typed[Float, Float4Vector] {
      override def set(vector: Float4Vector, index: Int, value: Float): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val shortEncoder: Encoder.Typed[Short, SmallIntVector] = {
    new Encoder.Typed[Short, SmallIntVector] {
      override def set(vector: SmallIntVector, index: Int, value: Short): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val ushortEncoder: Encoder.Typed[Char, UInt2Vector] = {
    new Encoder.Typed[Char, UInt2Vector] {
      override def set(vector: UInt2Vector, index: Int, value: Char): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val byteEncoder: Encoder.Typed[Byte, UInt1Vector] = {
    new Encoder.Typed[Byte, UInt1Vector] {
      override def set(vector: UInt1Vector, index: Int, value: Byte): Unit = {
        vector.set(index, value)
      }
    }
  }

  implicit val byteArrayEncoder: Encoder.Typed[Array[Byte], VarBinaryVector] = {
    new Encoder.Typed[Array[Byte], VarBinaryVector] {
      override def set(vector: VarBinaryVector, index: Int, value: Array[Byte]): Unit = {
        vector.setSafe(index, value)
      }
    }
  }
}
