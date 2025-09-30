package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

trait PrimitiveArray extends ArrowArray {
  override def isPrimitive: Boolean = true

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false
}

object PrimitiveArray {
  abstract class Typed[V <: FieldVector, T : ru.TypeTag] extends ArrowArray.Typed[V, T] with PrimitiveArray {

  }
}
