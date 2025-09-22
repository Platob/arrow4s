package io.github.platob.arrow4s.core.decode

import org.apache.arrow.vector.FieldVector

trait Decoder {
  def getAny(vector: FieldVector, index: Int): Any
}

object Decoder {
  abstract class Typed[T, V <: FieldVector] extends Decoder {
    def get(vector: V, index: Int): T

    def getOrNull(vector: V, index: Int): T = {
      if (vector.isNull(index)) null.asInstanceOf[T]
      else get(vector, index)
    }

    def getOptional(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None
      else Some(get(vector, index))
    }

    def getAny(vector: FieldVector, index: Int): Any = {
      this.getOrNull(vector.asInstanceOf[V], index)
    }
  }
}
