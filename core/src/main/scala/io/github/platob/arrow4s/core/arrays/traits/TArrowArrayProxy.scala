package io.github.platob.arrow4s.core.arrays.traits

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.ValueVector

trait TArrowArrayProxy extends TArrowArray {
  def inner: ArrowArray[_]

  override def vector: ValueVector = inner.vector

  override def length: Int = inner.length

  override def isPrimitive: Boolean = inner.isPrimitive

  override def isNested: Boolean = inner.isNested

  override def isOptional: Boolean = inner.isOptional

  override def isLogical: Boolean = inner.isLogical

  override def close(): Unit = inner.close()
}
