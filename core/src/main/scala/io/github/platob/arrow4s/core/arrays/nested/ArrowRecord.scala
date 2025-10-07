package io.github.platob.arrow4s.core.arrays.nested

abstract class ArrowRecord {
  def length: Int

  @inline def apply(index: Int): Any = get(index)

  @inline def get(index: Int): Any

  @inline def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  @inline def unsafeSet(index: Int, value: Any): Unit

  def toArray: Array[Any] = {
    Array.tabulate(length)(i => get(i))
  }
}

object ArrowRecord {
  @inline def view(array: StructArray, index: Int): ArrowRecord = {
    new ArrowRecord {
      override val length: Int = array.cardinality

      override def get(i: Int): Any =
        array.childAt(i).get(index)

      override def unsafeSet(i: Int, value: Any): Unit =
        array.childAt(i).unsafeSet(index, value)
    }
  }
}