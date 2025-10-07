package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

import scala.reflect.runtime.universe

trait ArraySlice extends ArrowArrayProxy {
  def startIndex: Int

  def endIndex: Int

  override def length: Int = endIndex - startIndex

  @inline def checkIndexArg(index: Int): Int = {
    val target = index + startIndex

    if (target < startIndex || target >= endIndex) {
      throw new IndexOutOfBoundsException(s"Index $index out of bounds for slice [$startIndex, $endIndex)")
    }

    target
  }

  override def scalaType: universe.Type = inner.scalaType
}

object ArraySlice {
  class Typed[V <: ValueVector, Outer](
    val inner: ArrowArray.Typed[V, Outer],
    val startIndex: Int,
    val endIndex: Int,
  ) extends ArrowArrayProxy.Typed[V, Outer, Outer] with ArraySlice {
    override lazy val children: Seq[Typed[_, _]] = inner.children
      .map(_.slice(start = startIndex, end = endIndex))

    override def get(index: Int): Outer = {
      inner.get(checkIndexArg(index))
    }

    override def set(index: Int, value: Outer): this.type = {
      inner.set(checkIndexArg(index), value)
      this
    }

    override def as(tpe: universe.Type): Typed[V, _] = {
      inner.as(tpe).slice(start = startIndex, end = endIndex)
    }

    override def toArray(start: Int, size: Int): Array[Outer] = {
      val s = checkIndexArg(start)

      inner.toArray(start = s, size = size)
    }
  }
}
