package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.cast.AnyOpsPlus
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}

trait ArrowArray extends AutoCloseable {
  // Properties
  def vector: FieldVector

  def isPrimitive: Boolean

  def field: Field = vector.getField

  def nullable: Boolean = field.isNullable

  def length: Int = vector.getValueCount

  // Memory management
  def ensureCapacity(capacity: Int): this.type = {
    if (capacity > vector.getValueCapacity) {
      vector.setInitialCapacity(capacity)
      vector.reAlloc()
    }

    this
  }

  def setValueCount(count: Int): this.type = {
    vector.setValueCount(count)

    this
  }

  // Mutation
  def setValue[Other](index: Int, value: Other)(implicit encoder: AnyOpsPlus[Other]): this.type

  def setOptionalValue[Other](index: Int, value: Option[Other])(implicit encoder: AnyOpsPlus[Other]): this.type = {
    value match {
      case Some(v) => setValue(index, v)
      case None    => vector.setNull(index)
    }

    this
  }

  def setValues[Other](startIndex: Int, values: Iterable[Other])(implicit encoder: AnyOpsPlus[Other]): this.type = {
    var i = startIndex

    values.foreach { v =>
      setValue(i, v)
      i += 1
    }

    this
  }

  def setOptionalValues[Other](startIndex: Int, values: Iterable[Option[Other]])(implicit encoder: AnyOpsPlus[Other]): this.type = {
    var i = startIndex

    values.foreach { v =>
      setOptionalValue(i, v)
      i += 1
    }

    this
  }

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  trait Typed[T, V <: FieldVector] extends ArrowArray {
    def converter: AnyOpsPlus[T]

    def vector: V

    // Accessors
    def apply(index: Int): T = getValue(index)

    def getValue(index: Int): T

    def getOption(index: Int): Option[T] = {
      if (vector.isNull(index)) None
      else Some(getValue(index))
    }

    // Mutators
    def setValue(index: Int, value: T): this.type

    def setValues(startIndex: Int, values: Iterable[T]): this.type = {
      var i = startIndex

      values.foreach { v =>
        setValue(i, v)
        i += 1
      }

      this
    }

    // Getters
    def toSeq: Seq[T] = (0 until length).map(getValue)

    def toSeqOption: Seq[Option[T]] = (0 until length).map(getOption)
  }

  class Logical[L, T](
    val underlying: Typed[T, FieldVector],
    val toLogical: T => L,
    val toPhysical: L => T
  ) extends Typed[L, FieldVector] {
    val converter: AnyOpsPlus.Logical[L, T] = AnyOpsPlus
      .logical(to = toLogical, from = toPhysical)(underlying.converter)

    def vector: FieldVector = underlying.vector

    override def isPrimitive: Boolean = underlying.isPrimitive

    override def getValue(index: Int): L = toLogical(underlying.getValue(index))

    override def setValue(index: Int, value: L): this.type = {
      underlying.setValue(index, toPhysical(value))

      this
    }

    override def setValue[Other](startIndex: Int, value: Other)(implicit encoder: AnyOpsPlus[Other]): Logical.this.type = {
      underlying.setValue[Other](startIndex, value)(encoder)

      this
    }
  }

  // Factory methods
  def make[T : ru.TypeTag](values: T*)(implicit encoder: AnyOpsPlus[T]): ArrowArray.Typed[T, _] = {
    val field = ArrowField.fromScala[T]
    val allocator = new RootAllocator()
    val vector = emptyVector(field, allocator, values.size)
    val array = fromVector(vector)

    array.setValues[T](0, values)(encoder)

    array.asInstanceOf[ArrowArray.Typed[T, _]]
  }

  def makeOption[T : ru.TypeTag](values: Option[T]*)(implicit encoder: AnyOpsPlus[T]): ArrowArray.Typed[T, _] = {
    val field = ArrowField.fromScala[T]
    val allocator = new RootAllocator()
    val vector = emptyVector(field, allocator, values.size)
    val array = fromVector(vector)

    array.setOptionalValues[T](0, values)(encoder)

    array.asInstanceOf[ArrowArray.Typed[T, _]]
  }

  def fromVector(vector: ValueVector): ArrowArray = {
    val field = vector.getField
    val dtype = field.getType

    dtype.getTypeID match {
      case ArrowTypeID.Int =>
        val arrowType = dtype.asInstanceOf[ArrowType.Int]

        (arrowType.getBitWidth, arrowType.getIsSigned) match {
          case (8, true) =>
            new IntegralArray.ByteArray(vector.asInstanceOf[TinyIntVector])
          case (8, false) =>
            new IntegralArray.UByteArray(vector.asInstanceOf[UInt1Vector])
          case (16, true) =>
            new IntegralArray.ShortArray(vector.asInstanceOf[SmallIntVector])
          case (16, false) =>
            new IntegralArray.UShortArray(vector.asInstanceOf[UInt2Vector])
          case (32, true) =>
            new IntegralArray.IntArray(vector.asInstanceOf[IntVector])
          case (32, false) =>
            new IntegralArray.UIntArray(vector.asInstanceOf[UInt4Vector])
          case (64, true) =>
            new IntegralArray.LongArray(vector.asInstanceOf[BigIntVector])
          case (64, false) =>
            new IntegralArray.ULongArray(vector.asInstanceOf[UInt8Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported integer type: $arrowType")
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: $dtype")
    }
  }

  def emptyVector(
    field: Field,
    allocator: BufferAllocator,
    capacity: Int
  ): FieldVector = {
    val vector = field.getType.getTypeID match {
      case ArrowTypeID.Int =>
        emptyIntegralVector(
          field = field,
          arrowType = field.getType.asInstanceOf[ArrowType.Int],
          allocator = allocator,
          capacity = capacity
        )
      case ArrowTypeID.Bool =>
        val v = new BitVector(field, allocator)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Struct =>
        val v = new StructVector(field, allocator, null)

        v.setInitialCapacity(capacity)

        v
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }

    vector.setValueCount(capacity)

    vector
  }

  private def emptyIntegralVector(
    field: Field,
    arrowType: ArrowType.Int,
    allocator: BufferAllocator,
    capacity: Int
  ): FieldVector = {
    val v = (arrowType.getBitWidth, arrowType.getIsSigned) match {
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

    v.reAlloc()

    v
  }
}