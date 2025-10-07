package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.universe

trait ArraySlice extends ArrowArrayProxy {
  def startIndex: Int

  def endIndex: Int

  override def length: Int = endIndex - startIndex

  @inline def innerStartIndex(index: Int): Int = index + startIndex

  @inline def innerEndIndex(index: Int): Int = index + endIndex

  override def scalaType: universe.Type = inner.scalaType
}

object ArraySlice {
  class Typed[V <: ValueVector, Outer](
    val inner: ArrowArray.Typed[V, Outer],
    val startIndex: Int,
    val endIndex: Int
  ) extends ArrowArrayProxy.Typed[V, Outer, Outer] with ArraySlice {
    override lazy val children: Seq[Typed[_, _]] = inner.children
      .map(_.slice(start = startIndex, end = endIndex))

    override def get(index: Int): Outer = {
      inner.get(innerStartIndex(index))
    }

    override def set(index: Int, value: Outer): this.type = {
      inner.set(innerStartIndex(index), value)
      this
    }

    override def as(tpe: universe.Type): Typed[V, _] = {
      inner.as(tpe).slice(start = startIndex, end = endIndex)
    }

    override def toArray(start: Int, end: Int): Array[Outer] = {
      val s = innerStartIndex(start)
      val e = innerStartIndex(end)

      inner.toArray(start = s, end = e)
    }
  }
}
