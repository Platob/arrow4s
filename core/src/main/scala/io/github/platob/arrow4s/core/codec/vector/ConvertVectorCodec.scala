package io.github.platob.arrow4s.core.codec.vector

import io.github.platob.arrow4s.core.codec.convert.{FromValueConverter, ToValueConverter, ValueConverter}
import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}
import org.apache.arrow.vector.ValueVector

trait ConvertVectorCodec[In, Out] extends VectorCodec[Out] {
  def inner: VectorCodec[In]
}

object ConvertVectorCodec {
  abstract class Typed[In, Out, V <: ValueVector](
    val inner: VectorCodec.Typed[In, V],
    codec: ValueCodec[Out]
  ) extends VectorCodec.Typed[Out, V](codec) with ConvertVectorCodec[In, Out] {
    override def setNull(vector: V, index: Int): Unit = {
      inner.setNull(vector, index)
    }
  }

  class ToOption[In, Out, V <: ValueVector](
    val converter: ValueConverter[In, Out],
    inner: VectorCodec.Typed[In, V],
    codec: OptionalValueCodec[Out],
  ) extends Typed[In, Option[Out], V](inner, codec) {
    override def get(vector: V, index: Int): Option[Out] = {
      inner.getOption(vector, index).map(converter.encode)
    }

    override def set(vector: V, index: Int, value: Option[Out]): Unit = {
      inner.setOption(vector, index, value.map(converter.decode))
    }
  }

  class FromOption[In, Out, V <: ValueVector](
    val converter: FromValueConverter.FromOption[In, Out],
    inner: VectorCodec.Typed[Option[In], V],
  ) extends Typed[Option[In], Out, V](inner, converter.target) {
    override def get(vector: V, index: Int): Out = {
      val in = inner.get(vector, index)

      in.map(converter.inner.encode).getOrElse(codec.default)
    }

    override def set(vector: V, index: Int, value: Out): Unit = {
      val in = converter.decode(value)

      inner.set(vector, index, in)
    }
  }

  class BothOption[In, Out, V <: ValueVector](
    val converter: ValueConverter[In, Out],
    inner: VectorCodec.Typed[Option[In], V],
    codec: OptionalValueCodec[Out],
  ) extends Typed[Option[In], Option[Out], V](inner, codec) {
    override def get(vector: V, index: Int): Option[Out] = {
      inner.get(vector, index).map(converter.encode)
    }

    override def set(vector: V, index: Int, value: Option[Out]): Unit = {
      inner.set(vector, index, value.map(converter.decode))
    }
  }

  class NotNull[In, Out, V <: ValueVector](
    val converter: ValueConverter[In, Out],
    inner: VectorCodec.Typed[In, V],
    codec: ValueCodec[Out],
  ) extends Typed[In, Out, V](inner, codec) {
    override def get(vector: V, index: Int): Out = {
      converter.encode(inner.get(vector, index))
    }

    override def set(vector: V, index: Int, value: Out): Unit = {
      inner.set(vector, index, converter.decode(value))
    }
  }

  implicit def fromConverter[In, Out, V <: ValueVector](implicit
    converter: ValueConverter[In, Out],
    inner: VectorCodec.Typed[In, V],
    codec: ValueCodec[Out]
  ): VectorCodec.Typed[Out, V] = {
    val result = converter match {
      case toOpt: ToValueConverter.ToOption[i, o] =>
        val built = new ToOption[i, o, V](
          converter = toOpt.inner,
          inner = inner.asInstanceOf[VectorCodec.Typed[i, V]],
          codec = toOpt.target
        )

        built
      case fromOpt: FromValueConverter.FromOption[i, o] =>
        val built = new FromOption[i, o, V](
          converter = fromOpt,
          inner = inner.asInstanceOf[VectorCodec.Typed[Option[i], V]]
        )

        built
      case bothOpt: ValueConverter.BothOption[i, o] =>
        val built = new BothOption[i, o, V](
          converter = bothOpt.inner,
          inner = inner.asInstanceOf[VectorCodec.Typed[Option[i], V]],
          codec = bothOpt.target
        )

        built
      case _ =>
        val built = new NotNull[In, Out, V](
          converter = converter,
          inner = inner,
          codec = codec
        )

        built
    }

    result.asInstanceOf[VectorCodec.Typed[Out, V]]
  }
}
