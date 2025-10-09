package io.github.platob.arrow4s.core.values

import io.github.platob.arrow4s.core.codec.ValueCodec

import scala.reflect.runtime.{universe => ru}

trait ValueConverter[From, To] {
  val source: ValueCodec[From]
  val target: ValueCodec[To]
  def encode(value: To): From
  def decode(value: From): To
}

object ValueConverter {
  def apply[From, To](implicit vc: ValueConverter[From, To]): ValueConverter[From, To] = vc

  def create(from: ValueCodec[_], to: ValueCodec[_]): ValueConverter[_, _] = {
    from match {
      case codec: ValueCodec[t] @unchecked =>
        if (to.tpe =:= ru.typeOf[String]) {
          return toString(codec)
        }
        if (to.tpe =:= ru.typeOf[Array[Byte]]) {
          return toBytes(codec)
        }
        if (to.tpe =:= ru.typeOf[Boolean]) {
          return toBit(codec)
        }
        if (to.tpe =:= ru.typeOf[Byte]) {
          return toByte(codec)
        }
        if (to.tpe =:= ru.typeOf[UByte]) {
          return toByte(codec) // UByte handled as Byte
        }
        if (to.tpe =:= ru.typeOf[Short]) {
          return toShort(codec)
        }
        if (to.tpe =:= ru.typeOf[Char]) {
          return toChar(codec) // UShort handled as Short
        }
        if (to.tpe =:= ru.typeOf[Int]) {
          return toInt(codec)
        }
        if (to.tpe =:= ru.typeOf[UInt]) {
          return toInt(codec) // UInt handled as Int
        }
        if (to.tpe =:= ru.typeOf[Long]) {
          return toLong(codec)
        }
        if (to.tpe =:= ru.typeOf[ULong]) {
          return toLong(codec) // ULong handled as Long
        }
        if (to.tpe =:= ru.typeOf[java.math.BigInteger]) {
          return toBigInteger(codec)
        }
        if (to.tpe =:= ru.typeOf[Float]) {
          return toFloat(codec)
        }
        if (to.tpe =:= ru.typeOf[Double]) {
          return toDouble(codec)
        }
        if (to.tpe =:= ru.typeOf[java.math.BigDecimal]) {
          return toBigDecimal(codec)
        }
    }

    throw new IllegalArgumentException(s"Cannot create ValueConverter from $from to $to")
  }

  implicit def toBytes[T](implicit codec: ValueCodec[T]): ValueConverter[T, Array[Byte]] = {
    new ValueConverter[T, Array[Byte]] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Array[Byte]] = ValueCodec.bytes
      override def encode(value: Array[Byte]): T = source.fromBytes(value)
      override def decode(value: T): Array[Byte] = source.toBytes(value)
    }
  }
  implicit def fromBytes[T](implicit codec: ValueCodec[T]): ValueConverter[Array[Byte], T] = {
    new ValueConverter[Array[Byte], T] {
      override val source: ValueCodec[Array[Byte]] = ValueCodec.bytes
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Array[Byte] = codec.toBytes(value)
      override def decode(value: Array[Byte]): T = codec.fromBytes(value)
    }
  }

  implicit def toString[T](implicit codec: ValueCodec[T]): ValueConverter[T, String] = {
    new ValueConverter[T, String] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[String] = ValueCodec.string
      override def encode(value: String): T = source.fromString(value)
      override def decode(value: T): String = source.toString(value)
    }
  }
  implicit def fromString[T](implicit codec: ValueCodec[T]): ValueConverter[String, T] = {
    new ValueConverter[String, T] {
      override val source: ValueCodec[String] = ValueCodec.string
      override val target: ValueCodec[T] = codec
      override def encode(value: T): String = codec.toString(value)
      override def decode(value: String): T = codec.fromString(value)
    }
  }

  implicit def toBit[T](implicit codec: ValueCodec[T]): ValueConverter[T, Boolean] = {
    new ValueConverter[T, Boolean] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Boolean] = ValueCodec.bit
      override def encode(value: Boolean): T = source.fromBoolean(value)
      override def decode(value: T): Boolean = source.toBoolean(value)
    }
  }
  implicit def fromBit[T](implicit codec: ValueCodec[T]): ValueConverter[Boolean, T] = {
    new ValueConverter[Boolean, T] {
      override val source: ValueCodec[Boolean] = ValueCodec.bit
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Boolean = codec.toBoolean(value)
      override def decode(value: Boolean): T = codec.fromBoolean(value)
    }
  }

  implicit def toByte[T](implicit codec: ValueCodec[T]): ValueConverter[T, Byte] = {
    new ValueConverter[T, Byte] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Byte] = ValueCodec.byte
      override def encode(value: Byte): T = source.fromByte(value)
      override def decode(value: T): Byte = source.toByte(value)
    }
  }
  implicit def fromByte[T](implicit codec: ValueCodec[T]): ValueConverter[Byte, T] = {
    new ValueConverter[Byte, T] {
      override val source: ValueCodec[Byte] = ValueCodec.byte
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Byte = codec.toByte(value)
      override def decode(value: Byte): T = codec.fromByte(value)
    }
  }

  implicit def toUByte[T](implicit codec: ValueCodec[T]): ValueConverter[T, UByte] = {
    new ValueConverter[T, UByte] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[UByte] = ValueCodec.ubyte
      override def encode(value: UByte): T = source.fromUByte(value)
      override def decode(value: T): UByte = source.toUByte(value)
    }
  }
  implicit def fromUByte[T](implicit codec: ValueCodec[T]): ValueConverter[UByte, T] = {
    new ValueConverter[UByte, T] {
      override val source: ValueCodec[UByte] = ValueCodec.ubyte
      override val target: ValueCodec[T] = codec
      override def encode(value: T): UByte = codec.toUByte(value)
      override def decode(value: UByte): T = codec.fromUByte(value)
    }
  }

  implicit def toShort[T](implicit codec: ValueCodec[T]): ValueConverter[T, Short] = {
    new ValueConverter[T, Short] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Short] = ValueCodec.short
      override def encode(value: Short): T = source.fromShort(value)
      override def decode(value: T): Short = source.toShort(value)
    }
  }

  implicit def toChar[T](implicit codec: ValueCodec[T]): ValueConverter[T, Char] = {
    new ValueConverter[T, Char] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Char] = ValueCodec.char
      override def encode(value: Char): T = source.fromChar(value)
      override def decode(value: T): Char = source.toChar(value)
    }
  }
  implicit def fromChar[T](implicit codec: ValueCodec[T]): ValueConverter[Char, T] = {
    new ValueConverter[Char, T] {
      override val source: ValueCodec[Char] = ValueCodec.char
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Char = codec.toChar(value)
      override def decode(value: Char): T = codec.fromChar(value)
    }
  }

  implicit def toInt[T](implicit codec: ValueCodec[T]): ValueConverter[T, Int] = {
    new ValueConverter[T, Int] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Int] = ValueCodec.int
      override def encode(value: Int): T = source.fromInt(value)
      override def decode(value: T): Int = source.toInt(value)
    }
  }
  implicit def fromInt[T](implicit codec: ValueCodec[T]): ValueConverter[Int, T] = {
    new ValueConverter[Int, T] {
      override val source: ValueCodec[Int] = ValueCodec.int
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Int = codec.toInt(value)
      override def decode(value: Int): T = codec.fromInt(value)
    }
  }

  implicit def toUInt[T](implicit codec: ValueCodec[T]): ValueConverter[T, UInt] = {
    new ValueConverter[T, UInt] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[UInt] = ValueCodec.uint
      override def encode(value: UInt): T = source.fromUInt(value)
      override def decode(value: T): UInt = source.toUInt(value)
    }
  }
  implicit def fromUInt[T](implicit codec: ValueCodec[T]): ValueConverter[UInt, T] = {
    new ValueConverter[UInt, T] {
      override val source: ValueCodec[UInt] = ValueCodec.uint
      override val target: ValueCodec[T] = codec
      override def encode(value: T): UInt = codec.toUInt(value)
      override def decode(value: UInt): T = codec.fromUInt(value)
    }
  }

  implicit def toLong[T](implicit codec: ValueCodec[T]): ValueConverter[T, Long] = {
    new ValueConverter[T, Long] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Long] = ValueCodec.long
      override def encode(value: Long): T = source.fromLong(value)
      override def decode(value: T): Long = source.toLong(value)
    }
  }
  implicit def fromLong[T](implicit codec: ValueCodec[T]): ValueConverter[Long, T] = {
    new ValueConverter[Long, T] {
      override val source: ValueCodec[Long] = ValueCodec.long
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Long = codec.toLong(value)
      override def decode(value: Long): T = codec.fromLong(value)
    }
  }

  implicit def toULong[T](implicit codec: ValueCodec[T]): ValueConverter[T, ULong] = {
    new ValueConverter[T, ULong] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[ULong] = ValueCodec.ulong
      override def encode(value: ULong): T = source.fromULong(value)
      override def decode(value: T): ULong = source.toULong(value)
    }
  }
  implicit def fromULong[T](implicit codec: ValueCodec[T]): ValueConverter[ULong, T] = {
    new ValueConverter[ULong, T] {
      override val source: ValueCodec[ULong] = ValueCodec.ulong
      override val target: ValueCodec[T] = codec
      override def encode(value: T): ULong = codec.toULong(value)
      override def decode(value: ULong): T = codec.fromULong(value)
    }
  }

  implicit def toBigInteger[T](implicit codec: ValueCodec[T]): ValueConverter[T, java.math.BigInteger] = {
    new ValueConverter[T, java.math.BigInteger] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[java.math.BigInteger] = ValueCodec.bigInteger
      override def encode(value: java.math.BigInteger): T = source.fromBigInteger(value)
      override def decode(value: T): java.math.BigInteger = source.toBigInteger(value)
    }
  }
  implicit def fromBigInteger[T](implicit codec: ValueCodec[T]): ValueConverter[java.math.BigInteger, T] = {
    new ValueConverter[java.math.BigInteger, T] {
      override val source: ValueCodec[java.math.BigInteger] = ValueCodec.bigInteger
      override val target: ValueCodec[T] = codec
      override def encode(value: T): java.math.BigInteger = codec.toBigInteger(value)
      override def decode(value: java.math.BigInteger): T = codec.fromBigInteger(value)
    }
  }

  implicit def toFloat[T](implicit codec: ValueCodec[T]): ValueConverter[T, Float] = {
    new ValueConverter[T, Float] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Float] = ValueCodec.float
      override def encode(value: Float): T = source.fromFloat(value)
      override def decode(value: T): Float = source.toFloat(value)
    }
  }
  implicit def fromFloat[T](implicit codec: ValueCodec[T]): ValueConverter[Float, T] = {
    new ValueConverter[Float, T] {
      override val source: ValueCodec[Float] = ValueCodec.float
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Float = codec.toFloat(value)
      override def decode(value: Float): T = codec.fromFloat(value)
    }
  }

  implicit def toDouble[T](implicit codec: ValueCodec[T]): ValueConverter[T, Double] = {
    new ValueConverter[T, Double] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[Double] = ValueCodec.double
      override def encode(value: Double): T = source.fromDouble(value)
      override def decode(value: T): Double = source.toDouble(value)
    }
  }
  implicit def fromDouble[T](implicit codec: ValueCodec[T]): ValueConverter[Double, T] = {
    new ValueConverter[Double, T] {
      override val source: ValueCodec[Double] = ValueCodec.double
      override val target: ValueCodec[T] = codec
      override def encode(value: T): Double = codec.toDouble(value)
      override def decode(value: Double): T = codec.fromDouble(value)
    }
  }

  implicit def toBigDecimal[T](implicit codec: ValueCodec[T]): ValueConverter[T, java.math.BigDecimal] = {
    new ValueConverter[T, java.math.BigDecimal] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[java.math.BigDecimal] = ValueCodec.bigDecimal
      override def encode(value: java.math.BigDecimal): T = source.fromBigDecimal(value)
      override def decode(value: T): java.math.BigDecimal = source.toBigDecimal(value)
    }
  }
  implicit def fromBigDecimal[T](implicit codec: ValueCodec[T]): ValueConverter[java.math.BigDecimal, T] = {
    new ValueConverter[java.math.BigDecimal, T] {
      override val source: ValueCodec[java.math.BigDecimal] = ValueCodec.bigDecimal
      override val target: ValueCodec[T] = codec
      override def encode(value: T): java.math.BigDecimal = codec.toBigDecimal(value)
      override def decode(value: java.math.BigDecimal): T = codec.fromBigDecimal(value)
    }
  }
}
