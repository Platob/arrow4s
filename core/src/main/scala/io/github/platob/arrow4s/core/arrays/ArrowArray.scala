package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.cast.{AnyEncoder, LogicalEncoder}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
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

  def isOptional: Boolean

  def field: Field = vector.getField

  def nullable: Boolean = field.isNullable

  def length: Int = vector.getValueCount

  def currentType: ru.Type

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
  def as[C : ru.TypeTag]: ArrowArray.Typed[C, _] = {
    val casted = as(ru.typeOf[C])

    casted.asInstanceOf[ArrowArray.Typed[C, _]]
  }

  def as(tpe: ru.Type): ArrowArray

  def complexAs(tpe: ru.Type): ArrowArray =
    throw new UnsupportedOperationException(s"Complex casting is not supported for $this")

  def toOptional: OptionArray[_, _]

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  abstract class Typed[T : ru.TypeTag, V <: FieldVector] extends ArrowArray {
    def encoder: AnyEncoder.Arrow[T, V]

    def vector: V

    @inline def indices: Range = 0 until length

    @inline lazy val currentType: ru.Type = ru.typeOf[T]

    // Accessors
    @inline def apply(index: Int): T = getValue(index)

    @inline def getValue(index: Int): T = encoder.getVector(vector = this.vector, index = index)

    // Mutators
    def setValue(index: Int, value: T): this.type = {
      encoder.setVector(vector = this.vector, index = index, value = value)

      this
    }

    def setValues(startIndex: Int, values: Iterable[T]): this.type = {
      var i = startIndex

      values.foreach { v =>
        setValue(i, v)
        i += 1
      }

      this
    }

    // Collections
    def toSeq: IndexedSeq[T] = indices.map(getValue)

    // Cast
    override def as[C : ru.TypeTag]: ArrowArray.Typed[C, _] = {
      val castTo = ReflectUtils.getType[T]
      val casted = as(castTo)

      casted.asInstanceOf[ArrowArray.Typed[C, _]]
    }

    def as(castTo: ru.Type): ArrowArray = {
      if (this.currentType == castTo)
        return this

      (ReflectUtils.isOption(this.currentType), ReflectUtils.isOption(castTo)) match {
        case (true, true) =>
          val innerFrom = ReflectUtils.getTypeArgs(this.currentType).head
          val innerTo = ReflectUtils.getTypeArgs(castTo).head

          if (innerFrom == innerTo) {
            this
          } else {
            complexAs(innerTo).toOptional
          }
        case (true, false) =>
          // Unwrap option and cast inner type
          val innerFrom = ReflectUtils.getTypeArgs(this.currentType).head

          if (innerFrom == castTo) {
            this
          } else {
            complexAs(castTo)
          }
        case (false, true) =>
          // Wrap in option
          if (this.currentType == ReflectUtils.getTypeArgs(castTo).head) {
            this.toOptional
          } else {
            complexAs(ReflectUtils.getTypeArgs(castTo).head).toOptional
          }
        case (false, false) =>
          // Direct cast
          complexAs(castTo)
      }
    }

    override def toOptional: OptionArray[T, V] = {
      val optEncoder = LogicalEncoder.optionalArrow[T, V](encoder)

      new OptionArray[T, V](
        array = this,
        encoder = optEncoder
      )
    }
  }

  // Factory methods
  def apply[T : ru.TypeTag](vector: FieldVector): ArrowArray.Typed[T, _] = {
    val array = fromVector(vector).as[T]

    array
  }

  def apply[T : ru.TypeTag](values: T*): ArrowArray.Typed[T, _] = make(values)

  def make[T : ru.TypeTag](values: Seq[T]): ArrowArray.Typed[T, _] = {
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

    v.reAlloc()

    v
  }
}