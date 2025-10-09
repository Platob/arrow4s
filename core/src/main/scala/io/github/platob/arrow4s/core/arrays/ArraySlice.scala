package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.traits.TArraySlice
import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.ValueVector

trait ArraySlice[T] extends ArrowArray[T] with TArraySlice {
  override def asUnchecked(codec: ValueCodec[_]): ArraySlice[_] = {
    inner.asUnchecked(codec).arrowSlice(start = startIndex, end = endIndex)
  }

  override def innerAs(codec: ValueCodec[_]): ArraySlice[_] = {
    this.inner.innerAs(codec).arrowSlice(start = startIndex, end = endIndex)
  }
}

object ArraySlice {
  class Typed[
    Value, ArrowVector <: ValueVector, Arr <: ArrowArray.Typed[Value, ArrowVector, Arr]
  ](
    val inner: Arr,
    val startIndex: Int,
    val endIndex: Int
  ) extends ArrowArrayProxy.Typed[
    ArrowVector,
    Value, Arr,
    Value, Typed[Value, ArrowVector, Arr]
  ] with ArraySlice[Value] {
    override def codec: ValueCodec[Value] = inner.codec

    override lazy val children: Seq[ArraySlice[_]] = inner.children
      .map(_.arrowSlice(start = startIndex, end = endIndex))

    override def get(index: Int): Value = {
      inner.get(innerStartIndex(index))
    }

    override def set(index: Int, value: Value): this.type = {
      inner.set(innerStartIndex(index), value)
      this
    }

    override def toArray(start: Int, end: Int): Array[Value] = {
      val s = innerStartIndex(start)
      val e = innerEndIndex(end)

      inner.toArray(start = s, end = e)
    }
  }

  def instance[Value, ArrowVector <: ValueVector, Arr <: ArrowArray.Typed[Value, ArrowVector, Arr]](
    array: Arr,
    start: Int,
    end: Int
  ): ArraySlice.Typed[Value, ArrowVector, Arr] = {
    new ArraySlice.Typed[Value, ArrowVector, Arr](
      inner = array,
      startIndex = start,
      endIndex = end
    )
  }
}
