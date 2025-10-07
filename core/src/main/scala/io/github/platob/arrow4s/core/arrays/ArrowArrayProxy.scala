package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait ArrowArrayProxy[Inner, Outer] extends ArrowArray[Outer] {
  def inner: ArrowArray[Inner]

  override def vector: ValueVector = inner.vector

  override def length: Int = inner.length

  override def isPrimitive: Boolean = inner.isPrimitive

  override def isNested: Boolean = inner.isNested

  override def isOptional: Boolean = inner.isOptional

  override def isLogical: Boolean = inner.isLogical

  override val cardinality: Int = inner.cardinality

  override def child(index: Int): ArrowArray[_] = inner.child(index)

  override def child(name: String): ArrowArray[_] = inner.child(name)

  // Mutators
  override def setNull(index: Int): this.type = {
    inner.setNull(index)
    this
  }

  // Casting
  override def as(tpe: ru.Type): ArrowArray[_] = {
    if (this.inner.scalaType =:= tpe) {
      return this.inner
    }

    this.inner.as(tpe)
  }

  override def innerAs(tpe: ru.Type): ArrowArray[_] = this.as(tpe)

  override def close(): Unit = inner.close()
}

object ArrowArrayProxy {
  abstract class Value[Inner, Outer] extends ArrowArray[Outer] {
    def inner: ArrowArray[Inner]

    override def vector: ValueVector = inner.vector

    override def length: Int = inner.length

    override def isPrimitive: Boolean = inner.isPrimitive

    override def isNested: Boolean = inner.isNested

    override def isOptional: Boolean = inner.isOptional

    override def isLogical: Boolean = inner.isLogical

    override val cardinality: Int = inner.cardinality

    override def child(index: Int): ArrowArray[_] = inner.child(index)

    override def child(name: String): ArrowArray[_] = inner.child(name)

    // Mutators
    override def setNull(index: Int): this.type = {
      inner.setNull(index)
      this
    }

    // Casting
    override def as(tpe: ru.Type): ArrowArray[_] = {
      if (this.inner.scalaType =:= tpe) {
        return this.inner
      }

      this.inner.as(tpe)
    }

    override def innerAs(tpe: ru.Type): ArrowArray[_] = this.as(tpe)

    override def close(): Unit = inner.close()
  }

  trait Typed[Vector <: ValueVector, Inner, Outer]
    extends ArrowArray.Typed[Vector, Outer] with ArrowArrayProxy[Inner, Outer] {
    override def inner: ArrowArray.Typed[Vector, Inner]

    override def vector: Vector = inner.vector
  }
}
