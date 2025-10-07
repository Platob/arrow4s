package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.nested.StructArray
import io.github.platob.arrow4s.core.arrays.primitive.{BinaryArray, FloatingPointArray, IntegralArray}
import io.github.platob.arrow4s.core.memory.RootAllocatorExtension
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}

import scala.reflect.runtime.{universe => ru}

trait ArrowArray[ScalaType]
  extends AutoCloseable with scala.collection.immutable.IndexedSeq[ScalaType] {
  // Properties
  @inline def vector: ValueVector

  @inline def isPrimitive: Boolean

  @inline def isNested: Boolean

  @inline def isOptional: Boolean

  @inline def isLogical: Boolean

  @inline def field: Field = vector.getField

  @inline def nullable: Boolean = field.isNullable

  @inline def length: Int = vector.getValueCount

  val cardinality: Int = this.field.getChildren.size()

  def scalaType: ru.Type

  def child(index: Int): ArrowArray[_]

  def child(name: String): ArrowArray[_]

  // Memory management
  private def ensureIndex(index: Int): this.type = {
    if (index >= vector.getValueCapacity) {
      vector.reAlloc()
    }

    this
  }

  def setValueCount(count: Int): this.type = {
    vector.setValueCount(count)

    this
  }

  @inline def isNull(index: Int): Boolean = vector.isNull(index)

  // Accessors
  @inline def apply(index: Int): ScalaType = getOrNull(index)

  @inline def get(index: Int): ScalaType

  @inline def getOrNull(index: Int): ScalaType = {
    if (isNull(index)) null.asInstanceOf[ScalaType]
    else get(index)
  }

  // Mutation
  @inline def setNull(index: Int): this.type

  @inline def set(index: Int, value: ScalaType): this.type

  @inline def setOrNull(index: Int, value: ScalaType): this.type = {
    if (value == null) setNull(index)
    else set(index, value)
  }

  @inline def unsafeSet(index: Int, value: Any): this.type = {
    set(index, value.asInstanceOf[ScalaType])
  }

  @inline def setValues(index: Int, values: Iterable[ScalaType]): this.type = {
    val endIndex = index + values.size
    ensureIndex(endIndex)

    values.zipWithIndex.foreach { case (v, i) => set(index + i, v) }

    this
  }

  // Cast
  def as[C : ru.TypeTag]: ArrowArray[C] = {
    val casted = as(ru.typeOf[C].dealias)

    casted.asInstanceOf[ArrowArray[C]]
  }

  def as(tpe: ru.Type): ArrowArray[_] = {
    if (this.scalaType =:= tpe || ReflectUtils.implements(tpe, interface = this.scalaType)) {
      return this
    }

    if (ReflectUtils.isOption(tpe)) {
      val child = ReflectUtils.typeArgument(tpe, 0)

      return as(child).toOptional(tpe)
    }

    this.innerAs(tpe)
  }

  def innerAs(tpe: ru.Type): ArrowArray[_] = {
    throw new IllegalArgumentException(
      s"Cannot cast array from $scalaType to $tpe"
    )
  }

  def toOptional(scalaType: ru.Type): OptionArray[ScalaType] = {
    val arr = this

    new OptionArray[ScalaType] {
      override val scalaType: ru.Type = scalaType

      override val inner: ArrowArray[ScalaType] = arr
    }
  }

  override def slice(start: Int, end: Int): ArraySlice[ScalaType] = {
    val arr = this

    new ArraySlice[ScalaType] {
      override def startIndex: Int = start

      override def endIndex: Int = end

      override def inner: ArrowArray[ScalaType] = arr

      override val scalaType: ru.Type = arr.scalaType
    }
  }

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  trait Typed[V <: ValueVector, ScalaType] extends ArrowArray[ScalaType] {
    def vector: V

    // Cast
    override def as[C : ru.TypeTag]: ArrowArray.Typed[V, C] = {
      val casted = as(ru.typeOf[C].dealias)

      casted.asInstanceOf[ArrowArray.Typed[V, C]]
    }

    override def toOptional(scalaType: ru.Type): OptionArray.Typed[V, ScalaType] = {
      new OptionArray.Typed[V, ScalaType](
        scalaType = scalaType,
        inner = this
      )
    }

    override def slice(start: Int, end: Int): ArraySlice.Typed[V, ScalaType] = {
      ArraySlice.Typed[V, ScalaType](
        inner = this,
        startIndex = start,
        endIndex = end
      )
    }
  }

  // Factory methods
  def apply[T : ru.TypeTag](values: T*): ArrowArray[T] = make(values)

  private def make[T : ru.TypeTag](values: Seq[T]): ArrowArray[T] = {
    val field = ArrowField.fromScala[T]
    val allocator = RootAllocatorExtension.INSTANCE
    val vector = emptyVector(field, allocator, values.size)
    val array = from(vector).as[T]

    array.setValues(0, values)

    array
  }

  def from(vector: ValueVector): ArrowArray[_] = {
    val field = vector.getField
    val dtype = field.getType

    dtype.getTypeID match {
      case ArrowTypeID.Binary =>
        new BinaryArray.VarBinaryArray(vector.asInstanceOf[VarBinaryVector])
      case ArrowTypeID.Utf8 =>
        new BinaryArray.UTF8Array(vector.asInstanceOf[VarCharVector])
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
      case ArrowTypeID.FloatingPoint =>
        val arrowType = dtype.asInstanceOf[ArrowType.FloatingPoint]

        arrowType.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            new FloatingPointArray.FloatArray(vector.asInstanceOf[Float4Vector])
          case FloatingPointPrecision.DOUBLE =>
            new FloatingPointArray.DoubleArray(vector.asInstanceOf[Float8Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported floating point type: $arrowType")
        }
      case ArrowTypeID.Decimal =>
        val arrowType = dtype.asInstanceOf[ArrowType.Decimal]

        arrowType.getBitWidth match {
          case 128 =>
            new FloatingPointArray.Decimal128Array(vector.asInstanceOf[DecimalVector])
          case 256 =>
            new FloatingPointArray.Decimal256Array(vector.asInstanceOf[Decimal256Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported decimal type: $arrowType")
        }
      case ArrowTypeID.Bool =>
        new IntegralArray.BooleanArray(vector.asInstanceOf[BitVector])
      case ArrowTypeID.Timestamp =>
        new IntegralArray.TimestampArray(vector.asInstanceOf[TimeStampVector])
      case ArrowTypeID.Date =>
        new IntegralArray.DateDayArray(vector.asInstanceOf[DateDayVector])
      case ArrowTypeID.Struct =>
        StructArray.default(vector.asInstanceOf[StructVector])
      case ArrowTypeID.List =>
        nested.ListArray.default(vector.asInstanceOf[ListVector])
      case ArrowTypeID.Map =>
        nested.MapArray.default(vector.asInstanceOf[MapVector])
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
      case ArrowTypeID.Binary =>
        new VarBinaryVector(field, allocator)
      case ArrowTypeID.Utf8 =>
        new VarCharVector(field, allocator)
      case ArrowTypeID.Int =>
        emptyIntegralVector(
          field = field,
          arrowType = field.getType.asInstanceOf[ArrowType.Int],
          allocator = allocator,
          capacity = capacity
        )
      case ArrowTypeID.FloatingPoint =>
        val arrowType = field.getType.asInstanceOf[ArrowType.FloatingPoint]
        arrowType.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            new Float4Vector(field, allocator)
          case FloatingPointPrecision.DOUBLE =>
            new Float8Vector(field, allocator)
          case _ =>
            throw new IllegalArgumentException(s"Unsupported floating point type: $arrowType")
        }
      case ArrowTypeID.Bool =>
        new BitVector(field, allocator)
      case ArrowTypeID.Decimal =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Decimal]

        arrowType.getBitWidth match {
          case 128 =>
            new DecimalVector(field, allocator)
          case 256 =>
            new Decimal256Vector(field, allocator)
          case _ =>
            throw new IllegalArgumentException(s"Unsupported decimal type: $arrowType")
        }
      case ArrowTypeID.Timestamp =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Timestamp]

        arrowType.getUnit match {
          case TimeUnit.SECOND => new TimeStampSecVector(field, allocator)
          case TimeUnit.MILLISECOND => new TimeStampMilliVector(field, allocator)
          case TimeUnit.MICROSECOND => new TimeStampMicroVector(field, allocator)
          case TimeUnit.NANOSECOND => new TimeStampNanoVector(field, allocator)
          case _ => throw new IllegalArgumentException(s"Unsupported time unit: ${arrowType.getUnit}")
        }
      case ArrowTypeID.Date =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Date]

        arrowType.getUnit match {
          case DateUnit.DAY => new DateDayVector(field, allocator)
          case DateUnit.MILLISECOND => new DateMilliVector(field, allocator)
          case _ => throw new IllegalArgumentException(s"Unsupported time unit: ${arrowType.getUnit}")
        }
      case ArrowTypeID.Struct =>
        new StructVector(field, allocator, null)
      case ArrowTypeID.List =>
        new ListVector(field, allocator, null)
      case ArrowTypeID.Map =>
        new MapVector(field, allocator, null)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }

    vector.setInitialCapacity(capacity)
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