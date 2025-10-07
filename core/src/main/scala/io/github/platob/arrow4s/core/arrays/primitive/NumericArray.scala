package io.github.platob.arrow4s.core.arrays.primitive

import org.apache.arrow.vector.FieldVector

trait NumericArray[T] extends PrimitiveArray[T] {

}

object NumericArray {
  trait Typed[V <: FieldVector, T]
    extends PrimitiveArray.Typed[V, T] with NumericArray[T] {

  }
}