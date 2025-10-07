package io.github.platob.arrow4s.core.arrays.primitive

import org.apache.arrow.vector.{FieldVector, VarBinaryVector, VarCharVector}

import java.nio.charset.Charset
import scala.reflect.runtime.{universe => ru}

trait BinaryArray[V <: FieldVector, T] extends PrimitiveArray.Typed[V, T] {

}

object BinaryArray {
  class VarBinaryArray(override val vector: VarBinaryVector) extends BinaryArray[VarBinaryVector, Array[Byte]] {
    override val scalaType: ru.Type = ru.typeOf[Array[Byte]].dealias

    // Accessors
    override def get(index: Int): Array[Byte] = {
      vector.get(index)
    }

    // Mutators
    override def set(index: Int, value: Array[Byte]): this.type = {
      vector.setSafe(index, value)
      this
    }
  }

  class UTF8Array(override val vector: VarCharVector) extends BinaryArray[VarCharVector, String] {
    private val charset: Charset = Charset.forName("UTF-8")

    override val scalaType: ru.Type = ru.typeOf[String].dealias

    // Accessors
    override def get(index: Int): String = {
      val bytes = vector.get(index)

      new String(bytes, charset)
    }

    // Mutators
    override def set(index: Int, value: String): this.type = {
      val b = value.getBytes(charset)
      vector.setSafe(index, b)
      this
    }
  }
}