package io.github.platob.arrow4s.core.codec.convert

import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}

import scala.reflect.runtime.{universe => ru}

abstract class ValueConverter[From, To] {
  def source: ValueCodec[From]
  def target: ValueCodec[To]
  def decode(value: To): From
  def encode(value: From): To
}

object ValueConverter extends FromValueConverter {
  def apply[From, To](implicit vc: ValueConverter[From, To]): ValueConverter[From, To] = vc

  def fromCodecs[F, T](from: ValueCodec[F], to: ValueCodec[T]): ValueConverter[F, T] = {
    val built = if (from.tpe == to.tpe) {
      return new ValueConverter[F, T] {
        override val source: ValueCodec[F] = from
        override val target: ValueCodec[T] = to
        override def decode(value: T): F = value.asInstanceOf[F]
        override def encode(value: F): T = value.asInstanceOf[T]
      }
    } else if (to.tpe =:= ru.typeOf[Array[Byte]]) {
      toBytes[F](from)
    } else if (to.tpe =:= ru.typeOf[String]) {
      toString[F](from)
    } else if (to.tpe =:= ru.typeOf[Boolean]) {
      toBoolean[F](from)
    } else if (to.tpe =:= ru.typeOf[Byte]) {
      toByte[F](from)
    } else if (to.tpe =:= ru.typeOf[UByte]) {
      toUByte[F](from)
    } else if (to.tpe =:= ru.typeOf[Short]) {
      toShort[F](from)
    } else if (to.tpe =:= ru.typeOf[Char]) {
      toChar[F](from)
    } else if (to.tpe =:= ru.typeOf[Int]) {
      toInt[F](from)
    } else if (to.tpe =:= ru.typeOf[UInt]) {
      toUInt[F](from)
    } else if (to.tpe =:= ru.typeOf[Long]) {
      toLong[F](from)
    } else if (to.tpe =:= ru.typeOf[ULong]) {
      toULong[F](from)
    } else if (to.tpe =:= ru.typeOf[java.math.BigInteger]) {
      toBigInteger[F](from)
    } else if (to.tpe =:= ru.typeOf[Float]) {
      toFloat[F](from)
    } else if (to.tpe =:= ru.typeOf[Double]) {
      toDouble[F](from)
    } else if (to.tpe =:= ru.typeOf[java.math.BigDecimal]) {
      toBigDecimal[F](from)
    } else {
      (from.isOption, to.isOption) match {
        case (false, true) =>
          to match {
            case o: OptionalValueCodec[t] =>
              val base = fromCodecs[F, t](from, o.inner)

              toOption[F, t](base, o)
          }
        case (true, false) =>
          from match {
            case o: OptionalValueCodec[f] =>
              val base = fromCodecs[f, T](o.inner, to)

              fromOption[f, T](base, o)
          }
        case _ =>
          throw new IllegalArgumentException(s"Cannot build ValueConverter from ${from.tpe} to ${to.tpe}")
      }
    }

    built.asInstanceOf[ValueConverter[F, T]]
  }

  implicit def identity[T](implicit codec: ValueCodec[T]): ValueConverter[T, T] =
    new ValueConverter[T, T] {
      override val source: ValueCodec[T] = codec
      override val target: ValueCodec[T] = codec
      override def decode(value: T): T = value
      override def encode(value: T): T = value
    }
}
