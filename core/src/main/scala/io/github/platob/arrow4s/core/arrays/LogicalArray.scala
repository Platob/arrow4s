package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.primitive.PrimitiveArray
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.{FieldVector, ValueVector}

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

class LogicalArray[
  A <: ArrowArray.Typed[V, Inner],
  V <: ValueVector,
  Inner, Outer,
](
  val scalaType: ru.Type,
  val inner: A,
  val getter: (LogicalArray[A, V, Inner, Outer], Int) => Outer,
  val setter: (LogicalArray[A, V, Inner, Outer], Int, Outer) => Any,
  val children: Seq[ArrowArray.Typed[_, _]]
) extends ArrowArrayProxy.Typed[V, Inner, Outer] {
  override def isLogical: Boolean = true

  implicit lazy val classTag: ClassTag[Outer] = ReflectUtils.classTag[Outer](scalaType)

  override def get(index: Int): Outer = {
    getter(this, index)
  }

  override def set(index: Int, value: Outer): this.type = {
    setter(this, index, value)
    this
  }

  override def toArray(start: Int, size: Int): Array[Outer] = {
    (start until (start + size)).map(get).toArray
  }
}

object LogicalArray {
  def convertPrimitive[A <: PrimitiveArray.Typed[V, Inner], V <: FieldVector, Inner](
    arr: A,
    tpe: ru.Type
  ): LogicalArray[A, V, Inner, _] = {
    val ext = arr.typeExtension

    val result =
      if (tpe =:= ru.typeOf[String]) {
        instance[A, V, Inner, String](
          arr,
          ext.toString,
          ext.fromString,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Array[Byte]]) {
        instance[A, V, Inner, Array[Byte]](
          arr,
          ext.toBytes,
          ext.fromBytes,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Byte]) {
        instance[A, V, Inner, Byte](
          arr,
          ext.toByte,
          ext.fromByte,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Short]) {
        instance[A, V, Inner, Short](
          arr,
          ext.toShort,
          ext.fromShort,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Char]) {
        instance[A, V, Inner, Char](
          arr,
          ext.toChar,
          ext.fromChar,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Int]) {
        instance[A, V, Inner, Int](
          arr,
          ext.toInt,
          ext.fromInt,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Long]) {
        instance[A, V, Inner, Long](
          arr,
          ext.toLong,
          ext.fromLong,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Float]) {
        instance[A, V, Inner, Float](
          arr,
          ext.toFloat,
          ext.fromFloat,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Double]) {
        instance[A, V, Inner, Double](
          arr,
          ext.toDouble,
          ext.fromDouble,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[java.math.BigInteger]) {
        instance[A, V, Inner, java.math.BigInteger](
          arr,
          ext.toBigInteger,
          ext.fromBigInteger,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[java.math.BigDecimal]) {
        instance[A, V, Inner, java.math.BigDecimal](
          arr,
          ext.toBigDecimal,
          ext.fromBigDecimal,
          tpe
        )
      }
      else if (tpe =:= ru.typeOf[Boolean]) {
        instance[A, V, Inner, Boolean](
          arr,
          ext.toBoolean,
          ext.fromBoolean,
          tpe
        )
      }
      else {
        throw new IllegalArgumentException(s"Cannot convert primitive array of type ${arr.scalaType} to logical array of type $tpe")
      }

    result.asInstanceOf[LogicalArray[A, V, Inner, _]]
  }

  def instance[
    A <: ArrowArray.Typed[V, Inner],
    V <: ValueVector, Inner, Outer
  ](
    array: A,
    toLogical: Inner => Outer,
    toPhysical: Outer => Inner,
    tpe: ru.Type,
    childrenArrays: Seq[ArrowArray.Typed[_, _]] = Nil
  ): LogicalArray[A, V, Inner, Outer] = {
    val getter = (arr: LogicalArray[A, V, Inner, Outer], index: Int) => {
      val innerValue = arr.inner.get(index)

      toLogical(innerValue)
    }
    val setter = (arr: LogicalArray[A, V, Inner, Outer], index: Int, value: Outer) => {
      val physicalValue = toPhysical(value)
      arr.inner.set(index, physicalValue)
    }

    new LogicalArray[A, V, Inner, Outer](
      scalaType = tpe,
      inner = array,
      getter = getter,
      setter = setter,
      children = if (childrenArrays.nonEmpty) childrenArrays else array.children
    )
  }
}