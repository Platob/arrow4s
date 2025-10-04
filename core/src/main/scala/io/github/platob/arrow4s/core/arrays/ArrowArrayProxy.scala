package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait ArrowArrayProxy extends ArrowArray {
  def inner: ArrowArray

  override def vector: ValueVector = inner.vector

  override def isPrimitive: Boolean = inner.isPrimitive

  override def isNested: Boolean = inner.isNested

  override def isOptional: Boolean = inner.isOptional

  override def isLogical: Boolean = inner.isLogical

  override def cardinality: Int = inner.cardinality

  override def childVector(index: Int): ValueVector = inner.childVector(index)

  // Accessors
  override def isNull(index: Int): Boolean = inner.isNull(index)

  // Mutators
  override def setNull(index: Int): this.type = {
    inner.setNull(index)
    this
  }

  // Casting
  override def as(tpe: ru.Type): ArrowArray = {
    if (this.inner.scalaType =:= tpe) {
      return this.inner
    }

    this.inner.as(tpe)
  }

  override def innerAs(tpe: ru.Type): ArrowArray = this.as(tpe)

  override def close(): Unit = inner.close()
}

object ArrowArrayProxy {
  trait Typed[
    Vector <: ValueVector, Inner, Outer
  ] extends ArrowArray.Typed[Vector, Outer] with ArrowArrayProxy {
    override def inner: ArrowArray.Typed[Vector, Inner]

    override def vector: Vector = inner.vector
  }
}
