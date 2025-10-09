package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector._

abstract class FloatingPointArray[Value: ValueCodec, ArrowVector <: FieldVector, Arr <: FloatingPointArray[Value, ArrowVector, Arr]](
  vector: ArrowVector
) extends NumericArray.Typed[Value, ArrowVector, Arr](vector) {

}

object FloatingPointArray {
  class FloatArray(vector: Float4Vector) extends FloatingPointArray[Float, Float4Vector, FloatArray](vector) {
    // Accessors
    override def get(index: Int): Float = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Float): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toFloat(value))
    }
  }

  class DoubleArray(vector: Float8Vector) extends FloatingPointArray[Double, Float8Vector, DoubleArray](vector) {
    // Accessors
    override def get(index: Int): Double = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Double): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toDouble(value))
    }
  }

  class Decimal128Array(vector: DecimalVector) extends FloatingPointArray[java.math.BigDecimal, DecimalVector, Decimal128Array](vector) {
    // Accessors
    override def get(index: Int): java.math.BigDecimal = {
      vector.getObject(index)
    }

    // Mutators
    override def set(index: Int, value: java.math.BigDecimal): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toBigDecimal(value))
    }
  }

  class Decimal256Array(vector: Decimal256Vector) extends FloatingPointArray[java.math.BigDecimal, Decimal256Vector, Decimal256Array](vector) {
    // Accessors
    override def get(index: Int): java.math.BigDecimal = {
      vector.getObject(index)
    }

    // Mutators
    override def set(index: Int, value: java.math.BigDecimal): this.type = {
      vector.set(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toBigDecimal(value))
    }
  }
}