package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.primitive.PrimitiveArray
import org.apache.arrow.vector.{FieldVector, ValueVector}

import scala.reflect.runtime.{universe => ru}

abstract class LogicalArray[V <: ValueVector, Inner, Outer] extends ArrowArray.Typed[V, Outer] with ArrowArrayProxy.Typed[V, Inner, Outer] {
  override def isLogical: Boolean = true

  val scalaType: ru.Type

  val inner: ArrowArray.Typed[V, Inner]
}

object LogicalArray {
  def convertPrimitive[V <: FieldVector, Inner](
    arr: PrimitiveArray.Typed[V, Inner],
    tpe: ru.Type
  ): LogicalArray[V, Inner, _] = {
    val ext = arr.typeExtension

    val result =
      if (tpe =:= ru.typeOf[String]) {
        instance[V, Inner, String](
          arr,
          ext.toString,
          ext.fromString,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Array[Byte]]) {
        instance[V, Inner, Array[Byte]](
          arr,
          ext.toBytes,
          ext.fromBytes,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Byte]) {
        instance[V, Inner, Byte](
          arr,
          ext.toByte,
          ext.fromByte,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Short]) {
        instance[V, Inner, Short](
          arr,
          ext.toShort,
          ext.fromShort,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Char]) {
        instance[V, Inner, Char](
          arr,
          ext.toChar,
          ext.fromChar,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Int]) {
        instance[V, Inner, Int](
          arr,
          ext.toInt,
          ext.fromInt,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Long]) {
        instance[V, Inner, Long](
          arr,
          ext.toLong,
          ext.fromLong,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Float]) {
        instance[V, Inner, Float](
          arr,
          ext.toFloat,
          ext.fromFloat,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Double]) {
        instance[V, Inner, Double](
          arr,
          ext.toDouble,
          ext.fromDouble,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[java.math.BigInteger]) {
        instance[V, Inner, java.math.BigInteger](
          arr,
          ext.toBigInteger,
          ext.fromBigInteger,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[java.math.BigDecimal]) {
        instance[V, Inner, java.math.BigDecimal](
          arr,
          ext.toBigDecimal,
          ext.fromBigDecimal,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Boolean]) {
        instance[V, Inner, Boolean](
          arr,
          ext.toBoolean,
          ext.fromBoolean,
          tpe
        )
      }
      else {
        throw new IllegalArgumentException(s"Cannot convert primitive array of type ${arr.scalaType} to logical array of type $tpe")
      }

    result.asInstanceOf[LogicalArray[V, Inner, _]]
  }

  def instance[V <: ValueVector, Inner, Outer](
    array: ArrowArray.Typed[V, Inner],
    toLogical: Inner => Outer,
    toPhysical: Outer => Inner,
    tpe: ru.Type
  ): LogicalArray[V, Inner, Outer] = {
    new LogicalArray[V, Inner, Outer] {
      override val inner: ArrowArray.Typed[V, Inner] = array

      override val scalaType: ru.Type = tpe

      override def get(index: Int): Outer = toLogical(inner.get(index))

      override def set(index: Int, value: Outer): this.type = {
        val newInnerValue = toPhysical(value)
        inner.set(index, newInnerValue)
        this
      }
    }
  }
}