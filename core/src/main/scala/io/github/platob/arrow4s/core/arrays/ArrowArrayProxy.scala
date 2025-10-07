package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait ArrowArrayProxy extends ArrowArray {
  def inner: ArrowArray

  override def vector: ValueVector = inner.vector

  override def length: Int = inner.length

  override def isPrimitive: Boolean = inner.isPrimitive

  override def isNested: Boolean = inner.isNested

  override def isOptional: Boolean = inner.isOptional

  override def isLogical: Boolean = inner.isLogical

  override val cardinality: Int = inner.cardinality

  override def close(): Unit = inner.close()
}

object ArrowArrayProxy {
  abstract class Typed[V <: ValueVector, Inner, Outer]
    extends ArrowArray.Typed[V, Outer] with ArrowArrayProxy {
    override def inner: ArrowArray.Typed[V, Inner]

    override def vector: V = inner.vector

    // Mutators
    override def setNull(index: Int): this.type = {
      inner.setNull(index)
      this
    }

    // Casting
    override def as(tpe: ru.Type): ArrowArray.Typed[V, _] = {
      if (this.scalaType =:= tpe) {
        return this
      }

      if (this.inner.scalaType =:= tpe) {
        return this.inner
      }

      this.inner.as(tpe)
    }

    override def innerAs(tpe: ru.Type): ArrowArray.Typed[V, _] = this.as(tpe)
  }
}
