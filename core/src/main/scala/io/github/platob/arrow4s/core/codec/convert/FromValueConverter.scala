package io.github.platob.arrow4s.core.codec.convert

import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}

trait FromValueConverter extends ToValueConverter {
  implicit def fromOption[F, T](implicit
    vc: ValueConverter[F, T], fromOpt: OptionalValueCodec[F]
  ): FromValueConverter.FromOption[F, T] = {
    new FromValueConverter.FromOption[F, T](vc, fromOpt)
  }

  implicit def fromBytes[T](implicit codec: ValueCodec[T]): ValueConverter[Array[Byte], T] = {
    new ValueConverter[Array[Byte], T] {
      override val source: ValueCodec[Array[Byte]] = ValueCodec.binary
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Array[Byte] = target.toBytes(value)
      override def encode(value: Array[Byte]): T = target.fromBytes(value)
    }
  }

  implicit def fromString[T](implicit codec: ValueCodec[T]): ValueConverter[String, T] = {
    new ValueConverter[String, T] {
      override val source: ValueCodec[String] = ValueCodec.utf8
      override val target: ValueCodec[T] = codec
      override def decode(value: T): String = target.toString(value)
      override def encode(value: String): T = target.fromString(value)
    }
  }

  implicit def fromBoolean[T](implicit codec: ValueCodec[T]): ValueConverter[Boolean, T] = {
    new ValueConverter[Boolean, T] {
      override val source: ValueCodec[Boolean] = ValueCodec.boolean
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Boolean = target.toBoolean(value)
      override def encode(value: Boolean): T = target.fromBoolean(value)
    }
  }

  implicit def fromByte[T](implicit codec: ValueCodec[T]): ValueConverter[Byte, T] = {
    new ValueConverter[Byte, T] {
      override val source: ValueCodec[Byte] = ValueCodec.byte
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Byte = target.toByte(value)
      override def encode(value: Byte): T = target.fromByte(value)
    }
  }

  implicit def fromUByte[T](implicit codec: ValueCodec[T]): ValueConverter[UByte, T] = {
    new ValueConverter[UByte, T] {
      override val source: ValueCodec[UByte] = ValueCodec.ubyte
      override val target: ValueCodec[T] = codec
      override def decode(value: T): UByte = target.toUByte(value)
      override def encode(value: UByte): T = target.fromUByte(value)
    }
  }

  implicit def fromShort[T](implicit codec: ValueCodec[T]): ValueConverter[Short, T] = {
    new ValueConverter[Short, T] {
      override val source: ValueCodec[Short] = ValueCodec.short
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Short = target.toShort(value)
      override def encode(value: Short): T = target.fromShort(value)
    }
  }

  implicit def fromChar[T](implicit codec: ValueCodec[T]): ValueConverter[Char, T] = {
    new ValueConverter[Char, T] {
      override val source: ValueCodec[Char] = ValueCodec.char
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Char = target.toChar(value)
      override def encode(value: Char): T = target.fromChar(value)
    }
  }

  implicit def fromInt[T](implicit codec: ValueCodec[T]): ValueConverter[Int, T] = {
    new ValueConverter[Int, T] {
      override val source: ValueCodec[Int] = ValueCodec.int
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Int = target.toInt(value)
      override def encode(value: Int): T = target.fromInt(value)
    }
  }

  implicit def fromUInt[T](implicit codec: ValueCodec[T]): ValueConverter[UInt, T] = {
    new ValueConverter[UInt, T] {
      override val source: ValueCodec[UInt] = ValueCodec.uint
      override val target: ValueCodec[T] = codec
      override def decode(value: T): UInt = target.toUInt(value)
      override def encode(value: UInt): T = target.fromUInt(value)
    }
  }

  implicit def fromLong[T](implicit codec: ValueCodec[T]): ValueConverter[Long, T] = {
    new ValueConverter[Long, T] {
      override val source: ValueCodec[Long] = ValueCodec.long
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Long = target.toLong(value)
      override def encode(value: Long): T = target.fromLong(value)
    }
  }

  implicit def fromULong[T](implicit codec: ValueCodec[T]): ValueConverter[ULong, T] = {
    new ValueConverter[ULong, T] {
      override val source: ValueCodec[ULong] = ValueCodec.ulong
      override val target: ValueCodec[T] = codec
      override def decode(value: T): ULong = target.toULong(value)
      override def encode(value: ULong): T = target.fromULong(value)
    }
  }

  implicit def fromFloat[T](implicit codec: ValueCodec[T]): ValueConverter[Float, T] = {
    new ValueConverter[Float, T] {
      override val source: ValueCodec[Float] = ValueCodec.float
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Float = target.toFloat(value)
      override def encode(value: Float): T = target.fromFloat(value)
    }
  }

  implicit def fromDouble[T](implicit codec: ValueCodec[T]): ValueConverter[Double, T] = {
    new ValueConverter[Double, T] {
      override val source: ValueCodec[Double] = ValueCodec.double
      override val target: ValueCodec[T] = codec
      override def decode(value: T): Double = target.toDouble(value)
      override def encode(value: Double): T = target.fromDouble(value)
    }
  }

  implicit def fromBigInteger[T](implicit codec: ValueCodec[T]): ValueConverter[java.math.BigInteger, T] = {
    new ValueConverter[java.math.BigInteger, T] {
      override val source: ValueCodec[java.math.BigInteger] = ValueCodec.bigInteger
      override val target: ValueCodec[T] = codec
      override def decode(value: T): java.math.BigInteger = target.toBigInteger(value)
      override def encode(value: java.math.BigInteger): T = target.fromBigInteger(value)
    }
  }

  implicit def fromBigDecimal[T](implicit codec: ValueCodec[T]): ValueConverter[java.math.BigDecimal, T] = {
    new ValueConverter[java.math.BigDecimal, T] {
      override val source: ValueCodec[java.math.BigDecimal] = ValueCodec.bigDecimal
      override val target: ValueCodec[T] = codec
      override def decode(value: T): java.math.BigDecimal = target.toBigDecimal(value)
      override def encode(value: java.math.BigDecimal): T = target.fromBigDecimal(value)
    }
  }
}

object FromValueConverter {
  class FromOption[From, To](
    val inner: ValueConverter[From, To],
    val source: OptionalValueCodec[From]
  ) extends ValueConverter[Option[From], To] {
    override def target: ValueCodec[To] = inner.target

    override def decode(value: To): Option[From] = {
      if (value == null) {
        None
      } else {
        Some(inner.decode(value))
      }
    }

    override def encode(value: Option[From]): To = {
      value match {
        case Some(v) => inner.encode(v)
        case None    => throw new IllegalArgumentException("Cannot decode None to non-nullable type")
      }
    }
  }
}
