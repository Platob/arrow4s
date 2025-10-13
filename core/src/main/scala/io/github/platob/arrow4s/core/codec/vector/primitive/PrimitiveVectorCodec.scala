package io.github.platob.arrow4s.core.codec.vector.primitive

import io.github.platob.arrow4s.core.codec.value.primitive.PrimitiveValueCodec
import io.github.platob.arrow4s.core.codec.vector.VectorCodec
import org.apache.arrow.vector._

trait PrimitiveVectorCodec[T] extends VectorCodec[T] {

}

object PrimitiveVectorCodec {
  abstract class Typed[T, V <: FieldVector](implicit codec: PrimitiveValueCodec[T]) extends VectorCodec.Typed[T, V](codec) {
    override def setNull(vector: V, index: Int): Unit = {
      vector.setNull(index)
    }
  }
}