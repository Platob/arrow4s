package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.cast.TypeConverter
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}

trait ArrowArray extends AutoCloseable {
  override def toString: String = s"${this.getClass.getName}[$scalaType]($length)"

  // Properties
  def vector: FieldVector

  def isPrimitive: Boolean

  def isOptional: Boolean

  def isLogical: Boolean

  def field: Field = vector.getField

  def nullable: Boolean = field.isNullable

  def length: Int = vector.getValueCount

  def scalaType: ru.Type

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
  @inline def setNull(index: Int): this.type = {
    vector.setNull(index)

    this
  }

  // Cast
  def as[C : ru.TypeTag]: ArrowArray.Typed[_, C] = {
    val casted = as(ru.typeOf[C].dealias)

    casted.asInstanceOf[ArrowArray.Typed[_, C]]
  }

  def as(castTo: ru.Type): ArrowArray

  def toOptional(scalaType: ru.Type): OptionArray[_, _]

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  abstract class Typed[V <: FieldVector, Source] extends ArrowArray {
    def vector: V

    def scalaType: ru.Type

    @inline def indices: Range = 0 until length

    // Accessors
    @inline def apply(index: Int): Source = get(index)

    @inline def get(index: Int): Source

    @inline def getOrNull(index: Int): Source = {
      if (vector.isNull(index)) null.asInstanceOf[Source]
      else get(index)
    }

    // Mutators
    @inline def set(index: Int, value: Source): this.type

    @inline def setOrNull(index: Int, value: Source): this.type = {
      if (value == null) setNull(index)
      else set(index, value)

      this
    }

    @inline def setValues(startIndex: Int, values: Seq[Source]): this.type = {
      this.ensureCapacity(startIndex + values.size)

      values.zipWithIndex.foreach { case (v, i) => setOrNull(startIndex + i, v) }

      this
    }

    // Collections
    def toSeq: IndexedSeq[Source] = indices.map(get)

    // Cast
    override def as[C : ru.TypeTag]: ArrowArray.Typed[V, C] = {
      val casted = as(ru.typeOf[C].dealias)

      casted.asInstanceOf[ArrowArray.Typed[V, C]]
    }

    override def as(castTo: ru.Type): ArrowArray = {
      if (this.scalaType =:= castTo) {
        return this
      }

      if (ReflectUtils.isOption(castTo)) {
        val child = ReflectUtils.typeArgument(castTo, 0)

        return as(child).toOptional(castTo)
      }

      val converter = TypeConverter.get(source = this.scalaType, target = castTo)

      converter match {
        case tc: TypeConverter[Source, c] @unchecked =>
          new LogicalArray[V, Source, c](scalaType = tc.targetType, array = this, converter = tc)
      }
    }

    override def toOptional(scalaType: ru.Type): OptionArray[V, Source] = {
      new OptionArray[V, Source](
        scalaType = scalaType,
        array = this
      )
    }
  }

  // Factory methods
  def apply[T : ru.TypeTag](values: T*): ArrowArray.Typed[_, T] = make(values)

  def make[T : ru.TypeTag](values: Seq[T]): ArrowArray.Typed[_, T] = {
    val field = ArrowField.fromScala[T]
    val allocator = new RootAllocator()
    val vector = emptyVector(field, allocator, values.size)
    val array = fromVector(vector).as[T]

    array.setValues(0, values)

    array
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

    v
  }
}