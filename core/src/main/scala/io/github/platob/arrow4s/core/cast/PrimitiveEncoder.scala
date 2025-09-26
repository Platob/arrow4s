package io.github.platob.arrow4s.core.cast

import org.apache.arrow.vector.FieldVector

trait PrimitiveEncoder[T] extends AnyEncoder[T] {

}

object PrimitiveEncoder {
  trait Arrow[T, V <: FieldVector] extends PrimitiveEncoder[T] with AnyEncoder.Arrow[T, V] {

  }
}