package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.AnyEncoder
import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

trait PrimitiveArray extends ArrowArray {
  def isPrimitive: Boolean = true

  def isOptional: Boolean = false

  def encoder: AnyEncoder.Arrow[_, _]
}

object PrimitiveArray {
  abstract class Typed[T : ru.TypeTag, V <: FieldVector] extends ArrowArray.Typed[T, V] with PrimitiveArray {
    def encoder: AnyEncoder.Arrow[T, V]
  }
}
