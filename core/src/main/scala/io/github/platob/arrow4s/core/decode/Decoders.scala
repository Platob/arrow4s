package io.github.platob.arrow4s.core.decode

import org.apache.arrow.vector.IntVector

object Decoders {
  implicit val intDecoder: Decoder.Typed[Int, IntVector] = new Decoder.Typed[Int, IntVector] {
    override def get(vector: IntVector, index: Int): Int = vector.get(index)
  }
}
