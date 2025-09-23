package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.decode.{Decoder, Decoders}
import io.github.platob.arrow4s.core.encode.{Encoder, Encoders}
import org.apache.arrow.vector.IntVector

case class IntArray(vector: IntVector) extends ArrowArray.Typed[Int, IntVector, IntArray] {
  override def encoder: Encoder.Typed[Int, IntVector] = Encoders.intEncoder

  override def decoder: Decoder.Typed[Int, IntVector] = Decoders.intDecoder
}
