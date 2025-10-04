package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

abstract class LogicalArray[V <: ValueVector, Inner, Outer] extends ArrowArray.Typed[V, Outer] with ArrowArrayProxy.Typed[V, Inner, Outer] {
  override def isLogical: Boolean = true

  val scalaType: ru.Type

  val inner: ArrowArray.Typed[V, Inner]
}
