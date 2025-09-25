package io.github.platob.arrow4s.core.decode

import org.apache.arrow.vector.FieldVector

trait Decoder {

}

object Decoder {
  abstract class Typed[T, V <: FieldVector] extends Decoder {
    /**
     * Get the value at the given index.
     * @param vector Input vector
     * @param index Index to get
     * @return Value at the given index
     */
    @inline def get(vector: V, index: Int): T

    /**
     * Get the value at the given index, or null if the value is null.
     * @param vector Input vector
     * @param index Index to get
     * @return Value at the given index, or null
     */
    @inline def getOrNull(vector: V, index: Int): T = {
      if (vector.isNull(index)) null.asInstanceOf[T]
      else get(vector, index)
    }

    /**
     * Get the value at the given index, or None if the value is null.
     * @param vector Input vector
     * @param index Index to get
     * @return Value at the given index, or None
     */
    @inline def getOption(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None
      else Some(get(vector, index))
    }
  }
}
