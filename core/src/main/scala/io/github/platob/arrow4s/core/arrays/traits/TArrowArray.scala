package io.github.platob.arrow4s.core.arrays.traits

import io.github.platob.arrow4s.core.codec.vector.VectorCodec
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.types.pojo.Field

trait TArrowArray extends AutoCloseable {
  def codec: VectorCodec[_]

  @inline def vector: ValueVector

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

  @inline def isNull(index: Int): Boolean = vector.isNull(index)

  @inline def setNull(index: Int): this.type = {
    codec.unsafeSetNull(vector, index)

    this
  }

  // AutoCloseable
  def close(): Unit = vector.close()
}
