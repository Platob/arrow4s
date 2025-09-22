package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import org.apache.arrow.vector.complex.StructVector

class StructArray[T](
  val vector: StructVector,
  val encoder: Encoder.Typed[T, StructVector],
  val decoder: Decoder.Typed[T, StructVector]
) extends ArrowArray.Typed[T, StructVector]
