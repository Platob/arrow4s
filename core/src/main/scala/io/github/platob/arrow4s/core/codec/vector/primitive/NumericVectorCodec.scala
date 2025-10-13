package io.github.platob.arrow4s.core.codec.vector.primitive

import io.github.platob.arrow4s.core.codec.value.primitive.NumericValueCodec
import org.apache.arrow.vector.FieldVector

trait NumericVectorCodec[T] extends PrimitiveVectorCodec[T] {

}

object NumericVectorCodec {
  abstract class Typed[T: NumericValueCodec, V <: FieldVector] extends PrimitiveVectorCodec.Typed[T, V] {

  }
}
