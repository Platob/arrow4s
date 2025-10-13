package io.github.platob.arrow4s.core.codec.convert

import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}

abstract class ValueConverter[From, To] {
  def source: ValueCodec[From]
  def target: ValueCodec[To]
  def decode(value: To): From
  def encode(value: From): To
}

object ValueConverter extends FromValueConverter {
  class BothOption[From, To](
    val inner: ValueConverter[From, To],
    val source: OptionalValueCodec[From],
    val target: OptionalValueCodec[To],
  ) extends ValueConverter[Option[From], Option[To]] {
    override def decode(value: Option[To]): Option[From] = value.map(v => inner.decode(v))
    override def encode(value: Option[From]): Option[To] = value.map(v => inner.encode(v))
  }

  def apply[From, To](implicit vc: ValueConverter[From, To]): ValueConverter[From, To] = vc

  implicit def bothOption[From, To](implicit vc: ValueConverter[From, To]): BothOption[From, To] = {
    new BothOption(vc, vc.source.toOptionalCodec, vc.target.toOptionalCodec)
  }

  def fromCodecs[F, T](from: ValueCodec[F], to: ValueCodec[T]): ValueConverter[F, T] = {
    ???
  }
}
