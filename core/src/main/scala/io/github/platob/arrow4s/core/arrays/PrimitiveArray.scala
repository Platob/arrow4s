package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.AnyOpsPlus
import org.apache.arrow.vector.FieldVector

trait PrimitiveArray extends ArrowArray {
  def isPrimitive: Boolean = true

  def converter: AnyOpsPlus[_]
}

object PrimitiveArray {
  trait Typed[T, V <: FieldVector] extends PrimitiveArray with ArrowArray.Typed[T, V] {
    def converter: AnyOpsPlus[T]
  }
}
