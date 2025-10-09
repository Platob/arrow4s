package io.github.platob.arrow4s.core.arrays.traits

import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field

trait TArrowArray extends AutoCloseable {
  def codec: ValueCodec[_]

  @inline def vector: ValueVector

  @inline def isPrimitive: Boolean

  @inline def isNested: Boolean

  @inline def isOptional: Boolean

  @inline def isLogical: Boolean

  @inline def field: Field = vector.getField

  @inline def length: Int = vector.getValueCount

  @inline def nullCount: Int = vector.getNullCount

  // Memory management
  def ensureIndex(index: Int): this.type = {
    if (index >= vector.getValueCapacity) {
      vector.reAlloc()
    }

    this
  }

  def setValueCount(count: Int): this.type = {
    vector.setValueCount(count)

    this
  }

  // Accessors
  @inline def unsafeGet[T](index: Int): T

  // Mutators
  @inline def unsafeSet(index: Int, value: Any): Unit

  @inline def isNull(index: Int): Boolean = vector.isNull(index)

  @inline def setNull(index: Int): this.type

  // AutoCloseable
  def close(): Unit = vector.close()
}
