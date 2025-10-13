package io.github.platob.arrow4s.core.codec.vector

import io.github.platob.arrow4s.core.codec.value.ValueCodec
import org.apache.arrow.vector.ValueVector

trait OptionalVectorCodec[T] extends VectorCodec[Option[T]] {
  val inner: VectorCodec[T]
}

object OptionalVectorCodec {
  class Typed[T, V <: ValueVector](val inner: VectorCodec.Typed[T, V])(implicit codec: ValueCodec[Option[T]])
    extends VectorCodec.Typed[Option[T], V](codec) with OptionalVectorCodec[T] {

    override def get(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None
      else Some(inner.get(vector, index))
    }

    override def set(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => inner.set(vector, index, v)
        case None    => setNull(vector, index)
      }
    }

    override def setNull(vector: V, index: Int): Unit = {
      inner.setNull(vector, index)
    }
  }
}
