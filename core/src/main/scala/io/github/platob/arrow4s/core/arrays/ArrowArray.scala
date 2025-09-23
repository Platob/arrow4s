package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.Decoder
import io.github.platob.arrow4s.core.encode.Encoder
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, IntVector}

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
  trait ValueTyped[T, A <: ValueTyped[T, A]] extends ArrowArray with Seq[T] {
    def apply(index: Int): T = getOrNull(index)

    def get(index: Int): T

    def getOrNull(index: Int): T

    def set(index: Int, value: T): A

    def set(startIndex: Int, values: Iterable[T]): A

    def iterator: Iterator[T] = indices.iterator.map(get)
  }

  trait Typed[T, V <: FieldVector, A <: Typed[T, V, A]] extends ValueTyped[T, A] {
    def encoder: Encoder.Typed[T, V]

    def decoder: Decoder.Typed[T, V]

    def vector: V

    def get(index: Int): T = decoder.get(vector, index)

    def getOrNull(index: Int): T = decoder.getOrNull(vector, index)

    def set(index: Int, value: T): A = {
      encoder.set(vector, index, value)

      this.asInstanceOf[A]
    }

    def set(startIndex: Int, values: Iterable[T]): A = {
      encoder.setSafe(vector, startIndex, values)

      this.asInstanceOf[A]
    }
  }

  def apply[T : TypeTag](values: Seq[T]): ValueTyped[T, _] = build[T](values)

  def build[T : TypeTag]: ValueTyped[T, _] = {
    val allocator = new RootAllocator()

    build[T](allocator, 0)
  }

  def build[T : TypeTag](values: Seq[T]): ValueTyped[T, _] = {
    val allocator = new RootAllocator()

    val array = build[T](allocator, values.size)

    array.setAnyValues(startIndex = 0, values = values)

    array
  }

  def build[T : TypeTag](
    allocator: BufferAllocator,
    capacity: Int
  ): ValueTyped[T, _] = {
    val field = ArrowField.fromScala[T]

    empty(field, allocator, capacity).asInstanceOf[ArrowArray.ValueTyped[T, _]]
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