package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.{AnyEncoder, LogicalEncoder}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

class OptionArray[T : ru.TypeTag, V <: FieldVector](
  val array: ArrowArray.Typed[T, V],
  val encoder: AnyEncoder.Arrow[Option[T], V]
) extends ArrowArray.Typed[Option[T], V] {
  override def vector: V = array.vector

  override def isPrimitive: Boolean = array.isPrimitive

  override def isOptional: Boolean = true

  override def as(castTo: ru.Type): ArrowArray = {
    if (currentType == castTo) {
      this
    } else if (ReflectUtils.getTypeArgs(currentType).head == castTo) {
      array
    } else {
      array.as(castTo).toOptional
    }
  }

  override def complexAs(tpe: ru.Type): ArrowArray = this.as(tpe)

  override def close(): Unit = array.close()
}
