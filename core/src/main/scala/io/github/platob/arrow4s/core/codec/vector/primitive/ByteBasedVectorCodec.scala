package io.github.platob.arrow4s.core.codec.vector.primitive

import io.github.platob.arrow4s.core.codec.value.primitive.ByteBasedValueCodec
import io.github.platob.arrow4s.core.codec.vector.VectorCodec
import org.apache.arrow.vector._

trait ByteBasedVectorCodec[T] extends PrimitiveVectorCodec[T] {

}

object ByteBasedVectorCodec {
  abstract class Typed[T, V <: FieldVector](implicit codec: ByteBasedValueCodec[T]) extends VectorCodec.Typed[T, V](codec) {
    def setNull(vector: V, index: Int): Unit = {
      vector.setNull(index)
    }

    @inline def getByteArray(vector: V, index: Int): Array[Byte]

    @inline def setByteArray(vector: V, index: Int, value: Array[Byte]): Unit
  }

  class VarBinaryVectorCodec extends Typed[Array[Byte], VarBinaryVector] {
    override def getByteArray(vector: VarBinaryVector, index: Int): Array[Byte] =
      vector.get(index)

    override def get(vector: VarBinaryVector, index: Int): Array[Byte] =
      vector.get(index)

    override def set(vector: VarBinaryVector, index: Int, value: Array[Byte]): Unit =
      vector.set(index, value)

    override def setByteArray(vector: VarBinaryVector, index: Int, value: Array[Byte]): Unit =
      vector.set(index, value)
  }

  class UTF8VectorCodec extends Typed[String, VarCharVector] {
    override def getByteArray(vector: VarCharVector, index: Int): Array[Byte] =
      vector.get(index)

    override def get(vector: VarCharVector, index: Int): String = {
      val bytes = getByteArray(vector, index)

      if (bytes == null) ""
      else new String(bytes)
    }

    override def setByteArray(vector: VarCharVector, index: Int, value: Array[Byte]): Unit = {
      vector.set(index, value)
    }

    override def set(vector: VarCharVector, index: Int, value: String): Unit = {
      val bytes = value.getBytes(ByteBasedValueCodec.UTF8_CHARSET)

      setByteArray(vector, index, bytes)
    }
  }
}