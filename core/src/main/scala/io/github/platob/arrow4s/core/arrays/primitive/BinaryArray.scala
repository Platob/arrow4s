package io.github.platob.arrow4s.core
package arrays.primitive

import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.vector.{FieldVector, VarBinaryVector, VarCharVector}

import java.nio.charset.Charset

abstract class BinaryArray[Value : ValueCodec, ArrowVector <: FieldVector, Arr <: BinaryArray[Value, ArrowVector, Arr]](vector: ArrowVector)
  extends PrimitiveArray.Typed[Value, ArrowVector, Arr](vector) {

}

object BinaryArray {
  class VarBinaryArray(vector: VarBinaryVector) extends BinaryArray[Array[Byte], VarBinaryVector, VarBinaryArray](vector) {
    // Accessors
    override def get(index: Int): Array[Byte] = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Array[Byte]): this.type = {
      vector.setSafe(index, value)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toBytes(value))
    }
  }

  class UTF8Array(vector: VarCharVector) extends BinaryArray[String, VarCharVector, UTF8Array](vector) {
    private val charset: Charset = Charset.forName("UTF-8")

    // Accessors
    override def get(index: Int): String = {
      val bytes = vector.get(index)

      try {
        new String(bytes, charset)
      } catch {
        case _: NullPointerException =>
//          throw new IllegalStateException(s"Value at index $index is null")
          "" // Return empty string if null
      }
    }

    // Mutators
    override def set(index: Int, value: String): this.type = {
      val b = value.getBytes(charset)
      vector.setSafe(index, b)
      this
    }

    override def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type = {
      set(index, codec.toString(value))
    }
  }
}