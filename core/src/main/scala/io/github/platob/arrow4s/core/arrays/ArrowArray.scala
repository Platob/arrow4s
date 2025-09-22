package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, IntVector, types}

trait ArrowArray extends AutoCloseable {
  def vector: FieldVector

  def encoder: Encoder.Typed[_, _]

  def decoder: Decoder.Typed[_, _]

  def nullable: Boolean = this.vector.getField.isNullable

  def length: Int = vector.getValueCount

  def getAny(index: Int): Any = decoder.getAny(vector, index)

  def setAny(index: Int, value: Any): Unit = encoder.setAny(vector, index, value)

  def close(): Unit = vector.close()
}

object ArrowArray {
  trait Typed[T, V <: FieldVector] extends ArrowArray {
    def encoder: Encoder.Typed[T, V]

    def decoder: Decoder.Typed[T, V]

    def vector: V

    def get(index: Int): T = decoder.get(vector, index)

    def getOptional(index: Int): Option[T] = decoder.getOptional(vector, index)

    def set(index: Int, value: T): Unit = encoder.setValue(vector, index, value)

    def setOptional(index: Int, value: Option[T]): Unit = encoder.setOptional(vector, index, value)
  }

  def build(field: Field, allocator: BufferAllocator): ArrowArray = {
    field.getType match {
      case types.Types.MinorType.INT =>
        IntArray.int(field, Seq.empty, allocator)
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