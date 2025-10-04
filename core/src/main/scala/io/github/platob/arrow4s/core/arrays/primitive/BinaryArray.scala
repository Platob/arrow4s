package io.github.platob.arrow4s.core.arrays.primitive

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import org.apache.arrow.vector.{FieldVector, VarBinaryVector, VarCharVector}

import java.nio.charset.Charset
import scala.reflect.runtime.{universe => ru}

trait BinaryArray[V <: FieldVector, T] extends PrimitiveArray.Typed[V, T] {
  override def innerAs(tpe: ru.Type): ArrowArray = {
    val arr = this

    if (tpe =:= ru.typeOf[Array[Byte]])
      new LogicalArray[V, T, Array[Byte]] {
        override val inner: BinaryArray[V, T] = arr

        override val scalaType: ru.Type = ru.typeOf[Array[Byte]].dealias

        override def get(index: Int): Array[Byte] = inner.getBytes(index)

        override def set(index: Int, value: Array[Byte]): this.type = {
          inner.setBytes(index, value)
          this
        }
      }
    else if (tpe =:= ru.typeOf[String])
      new LogicalArray[V, T, String] {
        override val inner: BinaryArray[V, T] = arr

        override val scalaType: ru.Type = ru.typeOf[String].dealias

        override def get(index: Int): String = {
          val bytes = inner.getBytes(index)

          new String(bytes, "UTF-8")
        }

        override def set(index: Int, value: String): this.type = {
          inner.setBytes(index, value.getBytes("UTF-8"))
          this
        }
      }
    else
      super.innerAs(tpe)
  }

  @inline def getBytes(index: Int): Array[Byte]

  @inline def getString(index: Int): String = new String(getBytes(index), "UTF-8")

  @inline def setBytes(index: Int, value: Array[Byte]): this.type

  @inline def setString(index: Int, value: String): this.type = setBytes(index, value.getBytes("UTF-8"))
}

object BinaryArray {
  class VarBinaryArray(override val vector: VarBinaryVector) extends BinaryArray[VarBinaryVector, Array[Byte]] {
    override val scalaType: ru.Type = ru.typeOf[Array[Byte]].dealias

    override def get(index: Int): Array[Byte] = {
      vector.get(index)
    }

    override def getBytes(index: Int): Array[Byte] = get(index)

    override def set(index: Int, value: Array[Byte]): this.type = {
      vector.setSafe(index, value)
      this
    }

    override def setBytes(index: Int, value: Array[Byte]): this.type = set(index, value)
  }

  class UTF8Array(override val vector: VarCharVector) extends BinaryArray[org.apache.arrow.vector.VarCharVector, String] {
    private val charset: Charset = Charset.forName("UTF-8")

    override val scalaType: ru.Type = ru.typeOf[String].dealias

    override def get(index: Int): String = {
      new String(getBytes(index), charset)
    }

    override def getBytes(index: Int): Array[Byte] = vector.get(index)

    override def set(index: Int, value: String): this.type = {
      val b = value.getBytes(charset)
      setBytes(index, b)
    }

    override def setBytes(index: Int, value: Array[Byte]): this.type = {
      vector.setSafe(index, value)
      this
    }
  }
}