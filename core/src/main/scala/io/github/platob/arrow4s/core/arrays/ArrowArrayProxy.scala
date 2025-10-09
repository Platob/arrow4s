package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.arrays.traits.TArrowArrayProxy
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.values.ValueConverter
import org.apache.arrow.vector.ValueVector

trait ArrowArrayProxy[T] extends ArrowArray[T] with TArrowArrayProxy {
  override def asUnchecked(codec: ValueCodec[_]): ArrowArray[_] = {
    if (this.codec == codec) {
      return this
    }

    if (this.inner.codec == codec.childAt(0).tpe) {
      return this.inner
    }

    this.inner.asUnchecked(codec)
  }

  override def innerAs(codec: ValueCodec[_]): ArrowArray[_] = this.asUnchecked(codec)
}

object ArrowArrayProxy {
  trait Typed[
    ArrowVector <: ValueVector,
    Inner, InArray <: ArrowArray.Typed[Inner, ArrowVector, InArray],
    Outer, Arr <: Typed[ArrowVector, Inner, InArray, Outer, Arr]
  ] extends ArrowArray.Typed[Outer, ArrowVector, Arr] with ArrowArrayProxy[Outer] {
    def inner: InArray

    override def vector: ArrowVector = inner.vector

    override def setNull(index: Int): Typed.this.type = {
      inner.setNull(index)
      this
    }

    // Casting
    override def as[C](implicit
      codec: ValueCodec[C],
      converter: ValueConverter[Outer, C]
    ): ArrowArray.Typed[C, ArrowVector, _] = {
      if (this.codec == codec) {
        return this.asInstanceOf[ArrowArray.Typed[C, ArrowVector, _]]
      }

      if (this.inner.codec == codec.tpe) {
        return this.inner.asInstanceOf[ArrowArray.Typed[C, ArrowVector, _]]
      }

      this.inner.asUnchecked(codec).asInstanceOf[ArrowArray.Typed[C, ArrowVector, _]]
    }
  }
}
