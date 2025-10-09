package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.FieldVector

trait NumericArray[T] extends PrimitiveArray[T] {

}

object NumericArray {
  abstract class Typed[Value : ValueCodec, ArrowVector <: FieldVector, Arr <: Typed[Value, ArrowVector, Arr]](vector: ArrowVector)
    extends PrimitiveArray.Typed[Value, ArrowVector, Arr](vector) with NumericArray[Value] {

  }
}