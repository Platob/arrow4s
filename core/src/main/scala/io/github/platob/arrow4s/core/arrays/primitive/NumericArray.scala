package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.extensions.TypeExtension
import org.apache.arrow.vector.FieldVector

trait NumericArray extends PrimitiveArray {

}

object NumericArray {
  abstract class Typed[V <: FieldVector, T : TypeExtension]
    extends PrimitiveArray.Typed[V, T] with NumericArray {

  }
}