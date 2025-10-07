package io.github.platob.arrow4s.core.arrays.primitive

import org.apache.arrow.vector.{Decimal256Vector, DecimalVector, FieldVector, Float4Vector, Float8Vector}

import scala.reflect.runtime.{universe => ru}

trait FloatingPointArray[V <: FieldVector, T] extends NumericArray.Typed[V, T] {

}

object FloatingPointArray {
  class FloatArray(override val vector: Float4Vector) extends FloatingPointArray[Float4Vector, Float] {
    override val scalaType: ru.Type = ru.typeOf[Float].dealias

    override def get(index: Int): Float = {
      vector.get(index)
    }

    override def set(index: Int, value: Float): this.type = {
      vector.set(index, value)
      this
    }
  }

  class DoubleArray(override val vector: Float8Vector) extends FloatingPointArray[Float8Vector, Double] {
    override val scalaType: ru.Type = ru.typeOf[Double].dealias

    override def get(index: Int): Double = {
      vector.get(index)
    }

    override def set(index: Int, value: Double): this.type = {
      vector.set(index, value)
      this
    }
  }

  class Decimal128Array(override val vector: DecimalVector) extends FloatingPointArray[DecimalVector, java.math.BigDecimal] {
    override val scalaType: ru.Type = ru.typeOf[BigDecimal].dealias

    override def get(index: Int): java.math.BigDecimal = {
      vector.getObject(index)
    }

    override def set(index: Int, value: java.math.BigDecimal): this.type = {
      vector.set(index, value)
      this
    }
  }

  class Decimal256Array(override val vector: Decimal256Vector) extends FloatingPointArray[Decimal256Vector, java.math.BigDecimal] {
    override val scalaType: ru.Type = ru.typeOf[BigDecimal].dealias

    override def get(index: Int): java.math.BigDecimal = {
      vector.getObject(index)
    }

    override def set(index: Int, value: java.math.BigDecimal): this.type = {
      vector.set(index, value)
      this
    }
  }
}