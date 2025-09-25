package io.github.platob.arrow4s.core.arrays

import io.github.platob.arrow4s.core.cast.AnyOpsPlus
import org.apache.arrow.vector.{FieldVector, VarBinaryVector}

trait BinaryArray[T, V <: FieldVector] extends PrimitiveArray.Typed[T, V] {
  def getValue(index: Int): T

  def setValue(index: Int, value: T): this.type

  def setValue[Other](index: Int, value: Other)(implicit encoder: AnyOpsPlus[Other]): this.type
}

object BinaryArray {
  class VarBinaryArray(override val vector: VarBinaryVector) extends BinaryArray[Array[Byte], VarBinaryVector] {
    override def converter: AnyOpsPlus[Array[Byte]] = ???

    override def getValue(index: Int): Array[Byte] = {
      vector.get(index)
    }

    override def setValue(index: Int, value: Array[Byte]): this.type = {
      vector.set(index, value)
      this
    }

    override def setValue[T](index: Int, value: T)(implicit encoder: AnyOpsPlus[T]): this.type = {
      this.setValue(index, encoder.toBytes(value))

      this
    }
  }
}
