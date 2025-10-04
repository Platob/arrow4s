package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

trait NumericArray extends PrimitiveArray {
  @inline def getInt(index: Int): Int = getLong(index).toInt

  @inline def getLong(index: Int): Long

  @inline def getFloat(index: Int): Float = getDouble(index).toFloat

  @inline def getDouble(index: Int): Double = getBigDecimal(index).doubleValue()

  @inline def getBigDecimal(index: Int): java.math.BigDecimal = java.math.BigDecimal.valueOf(getDouble(index))

  @inline def setInt(index: Int, value: Int): this.type = setLong(index, value.toLong)

  @inline def setLong(index: Int, value: Long): this.type

  @inline def setFloat(index: Int, value: Float): this.type = setDouble(index, value.toDouble)

  @inline def setDouble(index: Int, value: Double): this.type = setBigDecimal(index, java.math.BigDecimal.valueOf(value))

  @inline def setBigDecimal(index: Int, value:  java.math.BigDecimal): this.type = setDouble(index, value.doubleValue())
}

object NumericArray {
  trait Typed[V <: FieldVector, T]
    extends PrimitiveArray.Typed[V, T] with NumericArray {
    override def innerAs(tpe: ru.Type): ArrowArray = {
      val arr = this

      if (tpe =:= ru.typeOf[Int]) {
        new LogicalArray[V, T, Int] {
          override val scalaType: ru.Type = ru.typeOf[Int].dealias

          override val inner: Typed[V, T] = arr

          override def get(index: Int): Int = getInt(index)

          override def set(index: Int, value: Int): this.type = {
            setInt(index, value)

            this
          }
        }
      }
      else if (tpe =:= ru.typeOf[Long]) {
        new LogicalArray[V, T, Long] {
          override val scalaType: ru.Type = ru.typeOf[Long].dealias

          override val inner: Typed[V, T] = arr

          override def get(index: Int): Long = getLong(index)

          override def set(index: Int, value: Long): this.type = {
            setLong(index, value)

            this
          }
        }
      }
      else if (tpe =:= ru.typeOf[Float]) {
        new LogicalArray[V, T, Float] {
          override val scalaType: ru.Type = ru.typeOf[Float].dealias

          override val inner: Typed[V, T] = arr

          override def get(index: Int): Float = getFloat(index)

          override def set(index: Int, value: Float): this.type = {
            setFloat(index, value)

            this
          }
        }
      }
      else if (tpe =:= ru.typeOf[Double]) {
        new LogicalArray[V, T, Double] {
          override val scalaType: ru.Type = ru.typeOf[Double].dealias

          override val inner: Typed[V, T] = arr

          override def get(index: Int): Double = getDouble(index)

          override def set(index: Int, value: Double): this.type = {
            setDouble(index, value)

            this
          }
        }
      }
      else {
        super.innerAs(tpe)
      }
    }
  }
}