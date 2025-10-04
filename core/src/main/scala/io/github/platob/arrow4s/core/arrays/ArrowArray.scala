package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.nested.StructArray
import io.github.platob.arrow4s.core.arrays.primitive.{BinaryArray, FloatingPointArray, IntegralArray}
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}

trait ArrowArray extends AutoCloseable {
  override def toString: String = {
    val limit: Int = 10
    val anyValues = this.anyOrNullValues.take(limit).mkString(", ")
    val suffix = if (length > limit) ", ..." else ""

    s"[$anyValues$suffix]"
  }

  // Properties
  @inline def vector: ValueVector

  @inline def isPrimitive: Boolean

  @inline def isNested: Boolean

  @inline def isOptional: Boolean

  @inline def isLogical: Boolean

  @inline def field: Field = vector.getField

  @inline def nullable: Boolean = field.isNullable

  @inline def length: Int = vector.getValueCount

  @inline def cardinality: Int = this.field.getChildren.size()

  @inline def indices: Range = 0 until length

  @inline def childVector(index: Int): ValueVector

  @inline def childArray(index: Int): ArrowArray = {
    val v = childVector(index)

    ArrowArray.from(v)
  }

  def scalaType: ru.Type

  // Memory management
  def ensureCapacity(capacity: Int): this.type = {
    if (capacity > vector.getValueCapacity) {
      vector.reAlloc()
    }

    this
  }

  def setValueCount(count: Int): this.type = {
    vector.setValueCount(count)

    this
  }

  // Accessors
  private def anyOrNullValues: IndexedSeq[Any] = indices.map(getAnyOrNull)

  @inline def isNull(index: Int): Boolean

  @inline def getAny(index: Int): Any

  @inline def getAnyOrNull(index: Int): Any = {
    if (vector.isNull(index)) null
    else getAny(index)
  }

  @inline def getAnyOption(index: Int): Option[Any] = {
    if (vector.isNull(index)) None
    else Some(getAny(index))
  }

  // Mutation
  @inline def setAny(index: Int, value: Any): this.type

  @inline def setAnyOrNull(index: Int, value: Any): this.type = {
    if (value == null) setNull(index)
    else setAny(index, value)

    this
  }

  @inline def setNull(index: Int): this.type

  @inline def appendAny(value: Any): this.type

  // Cast
  def as[C : ru.TypeTag]: ArrowArray.Typed[_, C] = {
    val casted = as(ru.typeOf[C].dealias)

    casted.asInstanceOf[ArrowArray.Typed[_, C]]
  }

  def as(tpe: ru.Type): ArrowArray = {
    if (this.scalaType =:= tpe || ReflectUtils.implements(tpe, interface = this.scalaType)) {
      return this
    }

    if (ReflectUtils.isOption(tpe)) {
      val child = ReflectUtils.typeArgument(tpe, 0)

      return as(child).toOptional(tpe)
    }

    this.innerAs(tpe)
  }

  def innerAs(tpe: ru.Type): ArrowArray = {
    throw new IllegalArgumentException(
      s"Cannot cast array from $scalaType to $tpe"
    )
  }

  def toOptional(scalaType: ru.Type): OptionArray[_, _]

  def slice(start: Int, end: Int): ArraySlice

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  trait Typed[V <: ValueVector, ScalaType] extends ArrowArray with IndexedSeq[ScalaType] {
    def vector: V

    def scalaType: ru.Type

    override def indices: Range = 0 until length

    // Accessors
    @inline def apply(index: Int): ScalaType = getOrNull(index)

    @inline def get(index: Int): ScalaType

    override def getAny(index: Int): Any = get(index)

    @inline def getOrNull(index: Int): ScalaType = {
      if (isNull(index)) null.asInstanceOf[ScalaType]
      else get(index)
    }

    override def getAnyOrNull(index: Int): Any = getOrNull(index)

    @inline def getOption(index: Int): Option[ScalaType] = {
      if (isNull(index)) None
      else Some(get(index))
    }

    override def getAnyOption(index: Int): Option[Any] = getOption(index)

    // Mutators
    @inline def set(index: Int, value: ScalaType): this.type

    override def setAny(index: Int, value: Any): this.type = {
      set(index, value.asInstanceOf[ScalaType])
    }

    @inline def setOrNull(index: Int, value: ScalaType): this.type = {
      if (value == null) setNull(index)
      else set(index, value)

      this
    }

    @inline def setMany(startIndex: Int, values: Seq[ScalaType]): this.type = {
      values.zipWithIndex.foreach { case (v, i) => setOrNull(startIndex + i, v) }

      this
    }

    @inline def append(value: ScalaType): this.type = {
      val index = length

      ensureCapacity(index + 1)
      set(index, value)
      setValueCount(index + 1)

      this
    }

    @inline def appendMany(values: Seq[ScalaType]): this.type = {
      val startIndex = length
      val newLength = startIndex + values.length

      ensureCapacity(newLength)
      setMany(startIndex, values)
      setValueCount(newLength)

      this
    }

    override def appendAny(value: Any): this.type = {
      append(value.asInstanceOf[ScalaType])
    }

    // Cast
    override def as[C : ru.TypeTag]: ArrowArray.Typed[V, C] = {
      val casted = as(ru.typeOf[C].dealias)

      casted.asInstanceOf[ArrowArray.Typed[V, C]]
    }

    override def toOptional(scalaType: ru.Type): OptionArray[V, ScalaType] = {
      new OptionArray[V, ScalaType](
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
  def apply[T : ru.TypeTag](values: T*): ArrowArray.Typed[_, T] = make(values)

  def make[T : ru.TypeTag](values: Seq[T]): ArrowArray.Typed[_, T] = {
    val field = ArrowField.fromScala[T]
    val allocator = new RootAllocator()
    val vector = emptyVector(field, allocator, values.size)
    val array = from(vector).as[T]

    array.setMany(0, values)

    array
  }

  def from(vector: ValueVector): ArrowArray = {
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
        StructArray.from(vector.asInstanceOf[StructVector])
      case ArrowTypeID.List =>
        new nested.ListArray(vector.asInstanceOf[ListVector])
      case ArrowTypeID.Map =>
        new nested.MapArray(vector.asInstanceOf[MapVector])
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
        val v = new VarBinaryVector(field, allocator)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Utf8 =>
        val v = new VarCharVector(field, allocator)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Int =>
        emptyIntegralVector(
          field = field,
          arrowType = field.getType.asInstanceOf[ArrowType.Int],
          allocator = allocator,
          capacity = capacity
        )
      case ArrowTypeID.FloatingPoint =>
        val arrowType = field.getType.asInstanceOf[ArrowType.FloatingPoint]
        val v = arrowType.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            val v = new Float4Vector(field, allocator)

            v.setInitialCapacity(capacity)

            v
          case FloatingPointPrecision.DOUBLE =>
            val v = new Float8Vector(field, allocator)

            v.setInitialCapacity(capacity)

            v
          case _ =>
            throw new IllegalArgumentException(s"Unsupported floating point type: $arrowType")
        }

        v
      case ArrowTypeID.Bool =>
        val v = new BitVector(field, allocator)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Decimal =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Decimal]
        val v = arrowType.getBitWidth match {
          case 128 =>
            val v = new DecimalVector(field, allocator)

            v.setInitialCapacity(capacity)

            v
          case 256 =>
            val v = new Decimal256Vector(field, allocator)

            v.setInitialCapacity(capacity)

            v
          case _ =>
            throw new IllegalArgumentException(s"Unsupported decimal type: $arrowType")
        }

        v
      case ArrowTypeID.Timestamp =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Timestamp]

        val v = arrowType.getUnit match {
          case TimeUnit.SECOND => new TimeStampSecVector(field, allocator)
          case TimeUnit.MILLISECOND => new TimeStampMilliVector(field, allocator)
          case TimeUnit.MICROSECOND => new TimeStampMicroVector(field, allocator)
          case TimeUnit.NANOSECOND => new TimeStampNanoVector(field, allocator)
          case _ => throw new IllegalArgumentException(s"Unsupported time unit: ${arrowType.getUnit}")
        }

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Date =>
        val arrowType = field.getType.asInstanceOf[ArrowType.Date]

        val v = arrowType.getUnit match {
          case DateUnit.DAY => new DateDayVector(field, allocator)
          case DateUnit.MILLISECOND => new DateMilliVector(field, allocator)
          case _ => throw new IllegalArgumentException(s"Unsupported time unit: ${arrowType.getUnit}")
        }

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Struct =>
        val v = new StructVector(field, allocator, null)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.List =>
        val v = new ListVector(field, allocator, null)

        v.setInitialCapacity(capacity)

        v
      case ArrowTypeID.Map =>
        val v = new MapVector(field, allocator, null)

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