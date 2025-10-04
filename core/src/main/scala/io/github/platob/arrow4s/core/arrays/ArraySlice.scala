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
}

object ArraySlice {
  case class Typed[V <: ValueVector, Outer](
    inner: ArrowArray.Typed[V, Outer],
    startIndex: Int,
    endIndex: Int,
  ) extends ArrowArrayProxy.Typed[V, Outer, Outer] with ArraySlice {
    override def scalaType: universe.Type = inner.scalaType

    override def get(index: Int): Outer = {
      inner.get(checkIndexArg(index))
    }

    override def set(index: Int, value: Outer): this.type = {
      inner.set(checkIndexArg(index), value)
      this
    }

    override def as(tpe: universe.Type): ArraySlice = {
      inner.as(tpe).slice(start = startIndex, end = endIndex)
    }
  }
}
