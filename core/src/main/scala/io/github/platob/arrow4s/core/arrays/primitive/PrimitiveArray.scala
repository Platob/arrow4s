package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.entensions.TypeExtension
import org.apache.arrow.vector.{FieldVector, ValueVector}

import scala.reflect.runtime.{universe => ru}

abstract class PrimitiveArray[ScalaType : TypeExtension] extends ArrowArray[ScalaType] {
  override def isPrimitive: Boolean = true

  override def isNested: Boolean = false

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  val typeExtension: TypeExtension[ScalaType] = implicitly[TypeExtension[ScalaType]]

  override def scalaType: ru.Type = typeExtension.tpe

  override def child(index: Int): ArrowArray[_] =
    throw new UnsupportedOperationException("Primitive arrays do not have children")

  override def child(name: String): ArrowArray[_] =
    throw new UnsupportedOperationException("Primitive arrays do not have children")
}

object PrimitiveArray {
  abstract class Typed[V <: FieldVector, T : TypeExtension]
    extends PrimitiveArray[T] with ArrowArray.Typed[V, T] {

    override def setNull(index: Int): this.type = {
      vector.setNull(index)

      this
    }

    // Cast
    override def innerAs(tpe: ru.Type): ArrowArray[_] = {
      LogicalArray.convertPrimitive[V, T](arr = this, tpe = tpe)
    }
  }
}
