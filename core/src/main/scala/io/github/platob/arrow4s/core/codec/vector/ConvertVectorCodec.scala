package io.github.platob.arrow4s.core.codec.vector

import io.github.platob.arrow4s.core.codec.convert.{FromValueConverter, ToValueConverter, ValueConverter}
import io.github.platob.arrow4s.core.codec.value.OptionalValueCodec
import org.apache.arrow.vector.ValueVector

trait ConvertVectorCodec[In, Out] extends VectorCodec[Out] {
  def inner: VectorCodec[In]

  def converter: ValueConverter[In, Out]
}

object ConvertVectorCodec {
  class Typed[In, Out, V <: ValueVector](implicit
    val inner: VectorCodec.Typed[In, V],
    val converter: ValueConverter[In, Out]
  ) extends VectorCodec.Typed[Out, V](converter.target) with ConvertVectorCodec[In, Out] {
    private val (getter, setter): ((V, Int) => Out, (V, Int, Out) => Unit) = {
      (converter.source, converter.target) match {
        case (_: OptionalValueCodec[i], _: OptionalValueCodec[o]) =>
          val cas = converter.asInstanceOf[ValueConverter[Option[i], Option[o]]]

          val g: (V, Int) => Out = (v, i) => {
            val result: Option[o] = if (inner.isNull(v, i)) {
              cas.encode(None)
            } else {
              val in = inner.get(v, i).asInstanceOf[i]
              cas.encode(Some(in))
            }

            result.asInstanceOf[Out]
          }
          val s: (V, Int, Out) => Unit = (v, i, o) => {
            val out = o.asInstanceOf[Option[o]]
            
          }
      }
    }

    override def get(vector: V, index: Int): Out = {
      getter(vector, index)
    }

    override def set(vector: V, index: Int, value: Out): Unit = {
      setter(vector, index, value)
    }

    override def setNull(vector: V, index: Int): Unit = {
      inner.setNull(vector, index)
    }
  }
}
