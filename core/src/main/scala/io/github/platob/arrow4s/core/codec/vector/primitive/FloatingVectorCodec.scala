package io.github.platob.arrow4s.core.codec.vector.primitive

import io.github.platob.arrow4s.core.codec.value.primitive.FloatingValueCodec
import org.apache.arrow.vector._


trait FloatingVectorCodec[T] extends NumericVectorCodec[T] {

}

object FloatingVectorCodec {
  abstract class Typed[T: FloatingValueCodec, V <: FieldVector]
    extends NumericVectorCodec.Typed[T, V] with FloatingVectorCodec[T] {

  }

  class FloatVectorCodec extends Typed[Float, Float4Vector] {
    override def get(vector: Float4Vector, index: Int): Float =
      vector.get(index)

    override def set(vector: Float4Vector, index: Int, value: Float): Unit =
      vector.set(index, value)
  }

  class DoubleVectorCodec extends Typed[Double, Float8Vector] {
    override def get(vector: Float8Vector, index: Int): Double =
      vector.get(index)

    override def set(vector: Float8Vector, index: Int, value: Double): Unit =
      vector.set(index, value)
  }

  class BigDecimalVectorCodec extends Typed[java.math.BigDecimal, DecimalVector] {
    override def get(vector: DecimalVector, index: Int): java.math.BigDecimal =
      vector.getObject(index)

    override def set(vector: DecimalVector, index: Int, value: java.math.BigDecimal): Unit =
      vector.set(index, value)
  }
}