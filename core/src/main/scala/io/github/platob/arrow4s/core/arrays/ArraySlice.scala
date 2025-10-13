package io.github.platob.arrow4s.core.arrays

import org.apache.arrow.vector.ValueVector

trait ArraySlice[T] extends ArrowArray[T] {
  val startIndex: Int

  val endIndex: Int

  override def length: Int = endIndex - startIndex

  protected def innerStartIndex(index: Int): Int = {
    if (index < 0 || index >= length) {
      throw new IndexOutOfBoundsException(s"Index $index out of bounds for slice of length $length")
    }
    startIndex + index
  }

  protected def innerEndIndex(index: Int): Int = {
    if (index < 0 || index > length) {
      throw new IndexOutOfBoundsException(s"Index $index out of bounds for slice of length $length")
    }
    startIndex + index
  }
}

object ArraySlice {
  class Typed[Value, ArrowVector <: ValueVector](
    val inner: ArrowArray.Typed[Value, ArrowVector],
    val startIndex: Int,
    val endIndex: Int
  ) extends ArrowArray.Typed[Value, ArrowVector](inner.vector)(inner.codec) with ArraySlice[Value] {
    override def getObject(index: Int): Value = {
      inner.getObject(innerStartIndex(index))
    }

    override def setObject(index: Int, value: Value): this.type = {
      inner.setObject(innerStartIndex(index), value)
      this
    }

    override def toArray(start: Int, end: Int): Array[Value] = {
      val s = innerStartIndex(start)
      val e = innerEndIndex(end)

      inner.toArray(start = s, end = e)
    }
  }
}
