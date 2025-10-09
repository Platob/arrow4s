package io.github.platob.arrow4s.core.arrays.traits

trait TArraySlice extends TArrowArrayProxy {
  def startIndex: Int

  def endIndex: Int

  override def length: Int = endIndex - startIndex

  @inline def innerStartIndex(index: Int): Int = index + startIndex

  @inline def innerEndIndex(index: Int): Int = index + endIndex
}
