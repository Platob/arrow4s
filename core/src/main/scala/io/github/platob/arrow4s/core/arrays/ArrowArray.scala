package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, IntVector, types}

import scala.reflect.runtime.universe.TypeTag

trait ArrowArray extends AutoCloseable {
  def vector: FieldVector

  def encoder: Encoder.Typed[_, _]

  def decoder: Decoder.Typed[_, _]

  def nullable: Boolean = this.vector.getField.isNullable

  def length: Int = vector.getValueCount

  def getAnyValue(index: Int): Any = decoder.getAny(vector, index)

  def setAnyValue(index: Int, value: Any): this.type = {
    encoder.setAnyValue(vector, index, value)

    this
  }

  def setAnyValues(startIndex: Int, values: Iterable[Any]): this.type = {
    encoder.setAnyValues(vector, startIndex, values)

    this
  }

  def close(): Unit = vector.close()
}

object ArrowArray {
  trait Typed[T, V <: FieldVector, A <: Typed[T, V, A]]
    extends ArrowArray with Iterable[Option[T]] {
    def encoder: Encoder.Typed[T, V]

    def decoder: Decoder.Typed[T, V]

    def indices: Range = 0 until length

    def iterator: Iterator[Option[T]] = indices.iterator.map(getOption)

    def vector: V

    def get(index: Int): T = decoder.get(vector, index)

    def getOption(index: Int): Option[T] = decoder.getOption(vector, index)

    def set(index: Int, value: T): A = {
      encoder.set(vector, index, value)

      this.asInstanceOf[A]
    }

    def set(startIndex: Int, values: Iterable[T]): A = {
      encoder.setSafe(vector, startIndex, values)

      this.asInstanceOf[A]
    }
  }

  def build[T](implicit tt: TypeTag[T]): ArrowArray = {
    val allocator = new RootAllocator()

    build[T](allocator, 0)
  }

  def build[T](values: Seq[T])(implicit tt: TypeTag[T]): ArrowArray = {
    val allocator = new RootAllocator()

    val array = build[T](allocator, values.size)

    array.setAnyValues(startIndex = 0, values = values)

    array
  }

  def build[T](
    allocator: BufferAllocator,
    capacity: Int
  )(implicit tt: TypeTag[T]): ArrowArray = {
    val field = ArrowField.fromScala[T]

    empty(field, allocator, capacity)
  }

  def empty(
    field: Field,
    allocator: BufferAllocator,
    capacity: Int
  ): ArrowArray = {
    val dtype = field.getType

    dtype.getTypeID match {
      case ArrowTypeID.Int =>
        val vector = new IntVector(field, allocator)

        vector.setInitialCapacity(capacity)

        IntArray(vector)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }
}