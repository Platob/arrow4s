package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait OptionArray[ScalaType] extends ArrowArrayProxy[ScalaType, Option[ScalaType]] {
  override def isOptional: Boolean = true

  // Accessors
  override def get(index: Int): Option[ScalaType] = {
    if (inner.isNull(index)) None
    else Some(inner.get(index))
  }

  override def getOrNull(index: Int): Option[ScalaType] = get(index)

  // Mutators
  override def set(index: Int, value: Option[ScalaType]): this.type = {
    value match {
      case Some(v) => inner.set(index, v)
      case None    => inner.setNull(index)
    }

    this
  }
}

object OptionArray {
  class Typed[V <: ValueVector, Inner](
    val scalaType: ru.Type,
    val inner: ArrowArray.Typed[V, Inner],
  ) extends ArrowArrayProxy.Typed[V, Inner, Option[Inner]] with OptionArray[Inner] {

  }
}
