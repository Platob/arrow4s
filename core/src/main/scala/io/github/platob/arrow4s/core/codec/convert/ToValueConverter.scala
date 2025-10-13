package io.github.platob.arrow4s.core.codec.convert

import io.github.platob.arrow4s.core.codec.value.primitive.PrimitiveValueCodec
import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}

trait ToValueConverter {
  implicit def toBytes[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Array[Byte]] = {
    new ValueConverter[T, Array[Byte]] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Array[Byte]] = ValueCodec.binary
      override def decode(value: Array[Byte]): T = source.fromBytes(value)
      override def encode(value: T): Array[Byte] = source.toBytes(value)
    }
  }

  implicit def toString[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, String] = {
    new ValueConverter[T, String] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[String] = ValueCodec.utf8
      override def decode(value: String): T = source.fromString(value)
      override def encode(value: T): String = source.toString(value)
    }
  }

  implicit def toBoolean[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Boolean] = {
    new ValueConverter[T, Boolean] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Boolean] = ValueCodec.boolean
      override def decode(value: Boolean): T = source.fromBoolean(value)
      override def encode(value: T): Boolean = source.toBoolean(value)
    }
  }

  implicit def toByte[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Byte] = {
    new ValueConverter[T, Byte] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Byte] = ValueCodec.byte
      override def decode(value: Byte): T = source.fromByte(value)
      override def encode(value: T): Byte = source.toByte(value)
    }
  }

  implicit def toUByte[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, UByte] = {
    new ValueConverter[T, UByte] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[UByte] = ValueCodec.ubyte
      override def decode(value: UByte): T = source.fromUByte(value)
      override def encode(value: T): UByte = source.toUByte(value)
    }
  }

  implicit def toShort[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Short] = {
    new ValueConverter[T, Short] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Short] = ValueCodec.short
      override def decode(value: Short): T = source.fromShort(value)
      override def encode(value: T): Short = source.toShort(value)
    }
  }

  implicit def toChar[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Char] = {
    new ValueConverter[T, Char] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Char] = ValueCodec.char
      override def decode(value: Char): T = source.fromChar(value)
      override def encode(value: T): Char = source.toChar(value)
    }
  }

  implicit def toInt[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Int] = {
    new ValueConverter[T, Int] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Int] = ValueCodec.int
      override def decode(value: Int): T = source.fromInt(value)
      override def encode(value: T): Int = source.toInt(value)
    }
  }

  implicit def toUInt[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, UInt] = {
    new ValueConverter[T, UInt] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[UInt] = ValueCodec.uint
      override def decode(value: UInt): T = source.fromUInt(value)
      override def encode(value: T): UInt = source.toUInt(value)
    }
  }

  implicit def toLong[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Long] = {
    new ValueConverter[T, Long] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Long] = ValueCodec.long
      override def decode(value: Long): T = source.fromLong(value)
      override def encode(value: T): Long = source.toLong(value)
    }
  }

  implicit def toULong[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, ULong] = {
    new ValueConverter[T, ULong] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[ULong] = ValueCodec.ulong
      override def decode(value: ULong): T = source.fromULong(value)
      override def encode(value: T): ULong = source.toULong(value)
    }
  }

  implicit def toBigInteger[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, java.math.BigInteger] = {
    new ValueConverter[T, java.math.BigInteger] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[java.math.BigInteger] = ValueCodec.bigInteger
      override def decode(value: java.math.BigInteger): T = source.fromBigInteger(value)
      override def encode(value: T): java.math.BigInteger = source.toBigInteger(value)
    }
  }

  implicit def toFloat[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Float] = {
    new ValueConverter[T, Float] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Float] = ValueCodec.float
      override def decode(value: Float): T = source.fromFloat(value)
      override def encode(value: T): Float = source.toFloat(value)
    }
  }

  implicit def toDouble[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, Double] = {
    new ValueConverter[T, Double] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[Double] = ValueCodec.double
      override def decode(value: Double): T = source.fromDouble(value)
      override def encode(value: T): Double = source.toDouble(value)
    }
  }

  implicit def toBigDecimal[T](implicit codec: PrimitiveValueCodec[T]): ValueConverter[T, java.math.BigDecimal] = {
    new ValueConverter[T, java.math.BigDecimal] {
      override val source: PrimitiveValueCodec[T] = codec
      override val target: PrimitiveValueCodec[java.math.BigDecimal] = ValueCodec.bigDecimal
      override def decode(value: java.math.BigDecimal): T = source.fromBigDecimal(value)
      override def encode(value: T): java.math.BigDecimal = source.toBigDecimal(value)
    }
  }

  implicit def toOption[In, Out](implicit vc: ValueConverter[In, Out]): ToValueConverter.ToOption[In, Out] = {
    new ToValueConverter.ToOption[In, Out](vc, vc.target.toOptionalCodec)
  }
}

object ToValueConverter {
  class ToOption[From, To](
    val inner: ValueConverter[From, To],
    val target: OptionalValueCodec[To]
  ) extends ValueConverter[From, Option[To]] {
    override def source: ValueCodec[From] = inner.source

    override def decode(value: Option[To]): From = {
      value match {
        case Some(v) => inner.decode(v)
        case None    => source.default
      }
    }

    override def encode(value: From): Option[To] = {
      if (value == null) {
        None
      } else {
        Some(inner.encode(value))
      }
    }
  }
}