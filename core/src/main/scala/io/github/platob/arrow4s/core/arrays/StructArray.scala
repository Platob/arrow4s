package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.ArrowRecord
import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import org.apache.arrow.vector.complex.StructVector

import scala.jdk.CollectionConverters.CollectionHasAsScala

class StructArray(val vector: StructVector) extends ArrowArray.Typed[ArrowRecord, StructVector, StructArray] {
  override val encoder: Encoder.Typed[ArrowRecord, StructVector] = Encoder
    .struct(vector.getField.getChildren.asScala.toSeq)

  override val decoder: Decoder.Typed[ArrowRecord, StructVector] = Decoder
    .struct(vector.getField.getChildren.asScala.toSeq)
}
