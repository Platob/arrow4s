package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.{Decoder, Decoders}
import io.github.platob.arrow4s.core.encode.{Encoder, Encoders}
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.arrow.vector._

import scala.reflect.runtime.universe.TypeTag

trait ArrowArray extends AutoCloseable {
  def vector: FieldVector

  def encoder: Encoder.Typed[_, _]

  def decoder: Decoder.Typed[_, _]

  def nullable: Boolean = this.vector.getField.isNullable

  def length: Int = vector.getValueCount

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
  trait ValueTyped[T] extends ArrowArray with Seq[T] {
    def apply(index: Int): T = get(index)

    def get(index: Int): T

    def set(index: Int, value: T): this.type

    def set(startIndex: Int, values: Iterable[T]): this.type

    def iterator: Iterator[T] = indices.iterator.map(get)
  }

  trait Typed[T, V <: FieldVector] extends ValueTyped[T] {
    def encoder: Encoder.Typed[T, V]

    def decoder: Decoder.Typed[T, V]

    def vector: V

    def get(index: Int): T = decoder.get(vector, index)

    def set(index: Int, value: T): this.type = {
      encoder.set(vector, index, value)

      this
    }

    def set(startIndex: Int, values: Iterable[T]): this.type = {
      encoder.setSafe(vector, startIndex, values)

      this
    }
  }

  def apply[T : TypeTag](values: T*): ValueTyped[T] = build[T](values:_*)

  def build[T : TypeTag]: ValueTyped[T] = {
    val allocator = new RootAllocator()

    build[T](allocator, 0)
  }

  def build[T : TypeTag](values: T*): ValueTyped[T] = {
    val allocator = new RootAllocator()

    val array = build[T](allocator, values.size)

    array.setAnyValues(startIndex = 0, values = values)

    array
  }

  def build[T : TypeTag](
    allocator: BufferAllocator,
    capacity: Int
  ): ValueTyped[T] = {
    val field = ArrowField.fromScala[T]

    empty(field, allocator, capacity).asInstanceOf[ArrowArray.ValueTyped[T]]
  }

  def empty(
    field: Field,
    allocator: BufferAllocator,
    capacity: Int
  ): ArrowArray = {
    val dtype = field.getType

    dtype.getTypeID match {
      case ArrowTypeID.Int =>
        emptyInteger(
          field = field,
          allocator = allocator,
          capacity = capacity,
          arrowType = dtype.asInstanceOf[ArrowType.Int]
        )
      case ArrowTypeID.Bool =>
        val v = new BitVector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Boolean, BitVector] {
          override def encoder: Encoder.Typed[Boolean, BitVector] = Encoders.booleanEncoder

          override def decoder: Decoder.Typed[Boolean, BitVector] = Decoders.booleanDecoder

          override def vector: BitVector = v
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }

  private def emptyInteger(
    field: Field,
    allocator: BufferAllocator,
    capacity: Int,
    arrowType: ArrowType.Int
  ): ArrowArray.Typed[_, _] = {
    (arrowType.getBitWidth, arrowType.getIsSigned) match {
      case (8, true) =>
        throw new IllegalArgumentException(s"Arrow does not support signed 8-bit integers")
      case (8, false) =>
        val v = new UInt1Vector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Byte, UInt1Vector] {
          override def encoder: Encoder.Typed[Byte, UInt1Vector] = Encoders.byteEncoder

          override def decoder: Decoder.Typed[Byte, UInt1Vector] = Decoders.byteDecoder

          override def vector: UInt1Vector = v
        }
      case (16, true) =>
        val v = new SmallIntVector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Short, SmallIntVector] {
          override def encoder: Encoder.Typed[Short, SmallIntVector] = Encoders.shortEncoder

          override def decoder: Decoder.Typed[Short, SmallIntVector] = Decoders.shortDecoder

          override def vector: SmallIntVector = v
        }
      case (16, false) =>
        val v = new UInt2Vector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Char, UInt2Vector] {
          override def encoder: Encoder.Typed[Char, UInt2Vector] = Encoders.ushortEncoder

          override def decoder: Decoder.Typed[Char, UInt2Vector] = Decoders.ushortDecoder

          override def vector: UInt2Vector = v
        }
      case (32, true) =>
        val v = new IntVector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Int, IntVector] {
          override def encoder: Encoder.Typed[Int, IntVector] = Encoders.intEncoder

          override def decoder: Decoder.Typed[Int, IntVector] = Decoders.intDecoder

          override def vector: IntVector = v
        }
      case (32, false) =>
        val v = new UInt4Vector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Int, UInt4Vector] {
          override def encoder: Encoder.Typed[Int, UInt4Vector] = Encoders.uintEncoder

          override def decoder: Decoder.Typed[Int, UInt4Vector] = Decoders.uintDecoder

          override def vector: UInt4Vector = v
        }
      case (64, true) =>
        val v = new BigIntVector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Long, BigIntVector] {
          override def encoder: Encoder.Typed[Long, BigIntVector] = Encoders.longEncoder

          override def decoder: Decoder.Typed[Long, BigIntVector] = Decoders.longDecoder

          override def vector: BigIntVector = v
        }
      case (64, false) =>
        val v = new UInt8Vector(field, allocator)

        v.setInitialCapacity(capacity)

        new Typed[Long, UInt8Vector] {
          override def encoder: Encoder.Typed[Long, UInt8Vector] = Encoders.ulongEncoder

          override def decoder: Decoder.Typed[Long, UInt8Vector] = Decoders.ulongDecoder

          override def vector: UInt8Vector = v
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported integer type: $arrowType")
    }
  }
}