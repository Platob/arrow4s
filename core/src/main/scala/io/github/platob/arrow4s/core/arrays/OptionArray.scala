package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

class OptionArray[V <: ValueVector, T](
  val scalaType: ru.Type,
  val inner: ArrowArray.Typed[V, T],
) extends ArrowArrayProxy.Typed[V, T, Option[T]] {
  override def isOptional: Boolean = true

  // Accessors
  override def get(index: Int): Option[T] = {
    if (inner.isNull(index)) {
      None
    } else {
      Some(inner.get(index))
    }
  }

  // Mutators
  override def set(index: Int, value: Option[T]): this.type = {
    value match {
      case Some(v) => inner.set(index, v)
      case None => setNull(index)
    }

    this
  }
}
