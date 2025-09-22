package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, IntVector, types}

trait ArrowArray {
  def vector: FieldVector

  def encoder: Encoder.Typed[_, _]

  def decoder: Decoder.Typed[_, _]

  def nullable: Boolean = this.vector.getField.isNullable

  def length: Int = vector.getValueCount
}

object ArrowArray {
  trait Typed[T, V <: FieldVector] extends ArrowArray {
    def encoder: Encoder.Typed[T, V]

    def decoder: Decoder.Typed[T, V]

    def vector: V
  }

  def build(field: Field): ArrowArray = {
    field.getType match {
      case types.Types.MinorType.INT =>
        IntArray.int(field, Seq.empty, null) // Placeholder allocator
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }

  def from(vector: FieldVector): ArrowArray = {
    val field = vector.getField

    field.getType match {
      case types.Types.MinorType.INT =>
        IntArray(vector.asInstanceOf[IntVector])
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }
}