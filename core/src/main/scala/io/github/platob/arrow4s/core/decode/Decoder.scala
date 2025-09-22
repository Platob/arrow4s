package io.github.platob.arrow4s.core.decode

import org.apache.arrow.vector.FieldVector

trait Decoder {
  def unsafeGet(vector: FieldVector, index: Int): Any
}

object Decoder {
  abstract class Typed[T, V <: FieldVector] extends Decoder {
    def get(vector: V, index: Int): T

    def unsafeGet(vector: FieldVector, index: Int): Any = {
      this.get(vector.asInstanceOf[V], index)
    }
  }
}
