package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.decode.{Decoder, Decoders}
import io.github.platob.arrow4s.core.encode.{Encoder, Encoders}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.pojo.Field

case class IntArray(vector: IntVector) extends ArrowArray.Typed[Int, IntVector] {
  override def encoder: Encoder.Typed[Int, IntVector] = Encoders.intEncoder

  override def decoder: Decoder.Typed[Int, IntVector] = Decoders.intDecoder
}

object IntArray {
  def int(
    field: Field,
    values: Seq[Int],
    allocator: BufferAllocator
  ): IntArray = {
    val vector = new IntVector(field, allocator)

    vector.setValueCount(values.size)

    values.zipWithIndex.foreach { case (value, index) =>
      Encoders.intEncoder.setValue(vector, index, value)
    }

    IntArray(vector)
  }
}
