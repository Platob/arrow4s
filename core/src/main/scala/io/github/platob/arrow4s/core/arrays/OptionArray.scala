package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.{universe => ru}

trait OptionArray extends ArrowArrayProxy {
  override def isOptional: Boolean = true
}

object OptionArray {
  class Typed[V <: ValueVector, Inner](
    val scalaType: ru.Type,
    val inner: ArrowArray.Typed[V, Inner],
  ) extends ArrowArrayProxy.Typed[V, Inner, Option[Inner]] with OptionArray {
    override def children: Seq[ArrowArray.Typed[_, _]] = inner.children

    implicit lazy val classTag: scala.reflect.ClassTag[Option[Inner]] = scala.reflect.classTag[Option[Inner]]

    // Accessors
    override def get(index: Int): Option[Inner] = {
      if (inner.isNull(index)) None
      else Some(inner.get(index))
    }

    override def getOrNull(index: Int): Option[Inner] = get(index)

    // Mutators
    override def set(index: Int, value: Option[Inner]): this.type = {
      value match {
        case Some(v) => inner.set(index, v)
        case None    => inner.setNull(index)
      }

      this
    }

    override def toArray(start: Int, size: Int): Array[Option[Inner]] = {
      (start until (start + size)).map(get).toArray
    }
  }
}
