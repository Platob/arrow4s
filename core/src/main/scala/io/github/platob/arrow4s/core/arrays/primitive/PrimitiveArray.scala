package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.{FieldVector, ValueVector}

trait PrimitiveArray extends ArrowArray {
  override def isPrimitive: Boolean = true

  override def isNested: Boolean = false

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  override def isNull(index: Int): Boolean = vector.isNull(index)

  override def childVector(index: Int): ValueVector = {
    throw new IllegalArgumentException(s"$this is a primitive array and has no children")
  }
}

object PrimitiveArray {
  abstract class Typed[V <: FieldVector, T]
    extends ArrowArray.Typed[V, T] with PrimitiveArray {

    override def setNull(index: Int): this.type = {
      vector.setNull(index)

      this
    }
  }
}
