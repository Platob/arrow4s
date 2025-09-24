package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.decode.{Decoder, Decoders}
import io.github.platob.arrow4s.core.encode.{Encoder, Encoders}
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector

import scala.reflect.runtime.universe.TypeTag

trait ArrowArray extends AutoCloseable {
  override def toString: String = s"ArrowArray[${field.toString}, $length]"

  def vector: FieldVector

  def encoder: Encoder.Typed[_, _]

  def decoder: Decoder.Typed[_, _]

  // Properties
  def isOption: Boolean = this.decoder.isOptional

  def field: Field = vector.getField

  def nullable: Boolean = field.isNullable

  def length: Int = vector.getValueCount

  // Getters

  // Setters
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

    def toOption: Typed[Option[T], V] = {
      val e = Encoder.optional(this.encoder)
      val d = Decoder.optional(this.decoder)
      val v = this.vector

      new Typed[Option[T], V] {
        override val encoder: Encoder.Typed[Option[T], V] = e

        override val decoder: Decoder.Typed[Option[T], V] = d

        override val vector: V = v

        override val isOption: Boolean = true
      }
    }

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

  def apply[T : TypeTag](values: T*): Typed[T, _] = build[T](values:_*)

  def build[T : TypeTag](values: T*): Typed[T, _] = {
    val allocator = new RootAllocator()
    val field = ArrowField.fromScala[T]
    val vector = emptyVector(
      field = field,
      allocator = allocator,
      capacity = values.size
    )
    val array = fromVector(vector).asInstanceOf[Typed[T, _]]

    array.setAnyValues(startIndex = 0, values = values)

    array
  }

  def emptyVector(
    field: Field,
    allocator: BufferAllocator,
    capacity: Int
  ): FieldVector = {
    val vector = field.getType.getTypeID match {
      case ArrowTypeID.Int =>
        emptyIntegerVector(
          field = field,
          arrowType = field.getType.asInstanceOf[ArrowType.Int],
          allocator = allocator,
          capacity = capacity
        )
      case ArrowTypeID.Bool =>
        val v = new BitVector(field, allocator)

        v.setInitialCapacity(capacity)

        v
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }

    vector
  }

  private def emptyIntegerVector(
    field: Field,
    arrowType: ArrowType.Int,
    allocator: BufferAllocator,
    capacity: Int
  ): FieldVector = {
    (arrowType.getBitWidth, arrowType.getIsSigned) match {
      case (8, true) =>
        throw new IllegalArgumentException(s"Arrow does not support signed 8-bit integers")
      case (8, false) =>
        val v = new UInt1Vector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (16, true) =>
        val v = new SmallIntVector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (16, false) =>
        val v = new UInt2Vector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (32, true) =>
        val v = new IntVector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (32, false) =>
        val v = new UInt4Vector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (64, true) =>
        val v = new BigIntVector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case (64, false) =>
        val v = new UInt8Vector(field, allocator)
        v.setInitialCapacity(capacity)
        v
      case _ =>
        throw new IllegalArgumentException(s"Unsupported integer type: $arrowType")
    }
  }

  def fromVector(arrowVector: FieldVector): ArrowArray.Typed[_, _] = {
    val field = arrowVector.getField
    val dtype = field.getType

    val base = dtype.getTypeID match {
      case ArrowTypeID.Int =>
        integerArray(
          arrowVector = arrowVector,
          arrowType = dtype.asInstanceOf[ArrowType.Int]
        )
      case ArrowTypeID.Bool =>
        new Typed[Boolean, BitVector] {
          override def encoder: Encoder.Typed[Boolean, BitVector] = Encoders.booleanEncoder

          override def decoder: Decoder.Typed[Boolean, BitVector] = Decoders.booleanDecoder

          override val vector: BitVector = arrowVector.asInstanceOf[BitVector]
        }
      case ArrowTypeID.Struct =>
        new Typed[ArrowRecord, StructVector] {
          override def encoder: Encoder.Typed[ArrowRecord, StructVector] = Encoder.struct(field)

          override def decoder: Decoder.Typed[ArrowRecord, StructVector] = Decoder.struct(field)

          override val vector: StructVector = arrowVector.asInstanceOf[StructVector]
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }

    if (field.isNullable)
      base.toOption
    else
      base
  }

  private def integerArray(
    arrowVector: FieldVector,
    arrowType: ArrowType.Int
  ): ArrowArray.Typed[_, _] = {
    (arrowType.getBitWidth, arrowType.getIsSigned) match {
      case (8, true) =>
        throw new IllegalArgumentException(s"Arrow does not support signed 8-bit integers")
      case (8, false) =>
        new Typed[Byte, UInt1Vector] {
          override def encoder: Encoder.Typed[Byte, UInt1Vector] = Encoders.byteEncoder

          override def decoder: Decoder.Typed[Byte, UInt1Vector] = Decoders.byteDecoder

          override val vector: UInt1Vector = arrowVector.asInstanceOf[UInt1Vector]
        }
      case (16, true) =>
        new Typed[Short, SmallIntVector] {
          override def encoder: Encoder.Typed[Short, SmallIntVector] = Encoders.shortEncoder

          override def decoder: Decoder.Typed[Short, SmallIntVector] = Decoders.shortDecoder

          override val vector: SmallIntVector = arrowVector.asInstanceOf[SmallIntVector]
        }
      case (16, false) =>
        new Typed[Char, UInt2Vector] {
          override def encoder: Encoder.Typed[Char, UInt2Vector] = Encoders.ushortEncoder

          override def decoder: Decoder.Typed[Char, UInt2Vector] = Decoders.ushortDecoder

          override val vector: UInt2Vector = arrowVector.asInstanceOf[UInt2Vector]
        }
      case (32, true) =>
        new Typed[Int, IntVector] {
          override def encoder: Encoder.Typed[Int, IntVector] = Encoders.intEncoder

          override def decoder: Decoder.Typed[Int, IntVector] = Decoders.intDecoder

          override val vector: IntVector = arrowVector.asInstanceOf[IntVector]
        }
      case (32, false) =>
        new Typed[Int, UInt4Vector] {
          override def encoder: Encoder.Typed[Int, UInt4Vector] = Encoders.uintEncoder

          override def decoder: Decoder.Typed[Int, UInt4Vector] = Decoders.uintDecoder

          override val vector: UInt4Vector = arrowVector.asInstanceOf[UInt4Vector]
        }
      case (64, true) =>
        new Typed[Long, BigIntVector] {
          override def encoder: Encoder.Typed[Long, BigIntVector] = Encoders.longEncoder

          override def decoder: Decoder.Typed[Long, BigIntVector] = Decoders.longDecoder

          override val vector: BigIntVector = arrowVector.asInstanceOf[BigIntVector]
        }
      case (64, false) =>
        new Typed[Long, UInt8Vector] {
          override def encoder: Encoder.Typed[Long, UInt8Vector] = Encoders.ulongEncoder

          override def decoder: Decoder.Typed[Long, UInt8Vector] = Decoders.ulongDecoder

          override val vector: UInt8Vector = arrowVector.asInstanceOf[UInt8Vector]
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported integer type: $arrowType")
    }
  }
}