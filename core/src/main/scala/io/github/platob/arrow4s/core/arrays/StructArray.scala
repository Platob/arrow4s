package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.ArrowRecord
import io.github.platob.arrow4s.core.decode.{Decoder, Decoders}
import io.github.platob.arrow4s.core.encode.{Encoder, Encoders}
import org.apache.arrow.vector.complex.StructVector

class StructArray(val vector: StructVector) extends ArrowArray.Typed[ArrowRecord, StructVector, StructArray] {
  override def encoder: Encoder.Typed[ArrowRecord, StructVector] = Encoders.structEncoder

  override def decoder: Decoder.Typed[ArrowRecord, StructVector] = Decoders.structDecoder(vector.getField.getChildren)
}
