package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.nested.StructArray
import io.github.platob.arrow4s.core.arrays.primitive.{BinaryArray, FloatingPointArray, IntegralArray}
import io.github.platob.arrow4s.core.extensions.RootAllocatorExtension
import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}

trait ArrowArray extends AutoCloseable {
  // Properties
  @inline def vector: ValueVector

  @inline def isPrimitive: Boolean

  @inline def isNested: Boolean

  @inline def isOptional: Boolean

  @inline def isLogical: Boolean

  @inline def field: Field = vector.getField

  @inline def nullable: Boolean = field.isNullable

  @inline def length: Int = vector.getValueCount

  @inline def nullCount: Int = vector.getNullCount

  val cardinality: Int = this.field.getChildren.size()

  def scalaType: ru.Type

  // Memory management
  def ensureIndex(index: Int): this.type = {
    if (index >= vector.getValueCapacity) {
      vector.reAlloc()
    }

    this
  }

  def setValueCount(count: Int): this.type = {
    vector.setValueCount(count)

    this
  }

  // Accessors
  @inline def unsafeGet[T](index: Int): T

  // Mutators
  @inline def isNull(index: Int): Boolean = vector.isNull(index)

  @inline def setNull(index: Int): this.type

  @inline def unsafeSet(index: Int, value: Any): this.type

  // AutoCloseable
  def close(): Unit = vector.close()
}

object ArrowArray {
  abstract class Typed[V <: ValueVector, ScalaType]
    extends ArrowArray
      with scala.collection.immutable.IndexedSeq[ScalaType] {
    def vector: V

    def children: Seq[ArrowArray.Typed[_, _]]

    def childFields: Seq[Field] = children.map(_.field)

    @inline def childIndex(name: String): Int = {
      val index = childFields.indexWhere(_.getName == name)

      if (index == -1) {
        // Try case-insensitive match
        return childFields.indexWhere(_.getName.equalsIgnoreCase(name))
      }

      index
    }

    @throws[NoSuchElementException]
    @inline def childAt(index: Int): ArrowArray.Typed[_, _] = {
      try {
        children(index)
      } catch {
        case _: IndexOutOfBoundsException =>
          throw new NoSuchElementException(s"No child at index $index within ${childFields.map(_.getName).mkString("['", "', '", "']")}")
      }
    }

    @throws[NoSuchElementException]
    @inline def child(name: String): ArrowArray.Typed[_, _] = {
      val index = childIndex(name)

      try {
        children(index)
      } catch {
        case _: IndexOutOfBoundsException =>
          throw new NoSuchElementException(s"No child with name '$name' within ${childFields.map(_.getName).mkString("['", "', '", "']")}")
      }
    }

    @inline def findChild(name: String): Option[ArrowArray.Typed[_, _]] = {
      findChild(childIndex(name))
    }

    @inline def findChild(index: Int): Option[ArrowArray.Typed[_, _]] = {
      if (index < 0 || index >= children.size) None
      else Some(children(index))
    }

    // Accessors
    @inline def apply(index: Int): ScalaType = getOrNull(index)

    @inline def get(index: Int): ScalaType

    override def unsafeGet[T](index: Int): T = {
      get(index).asInstanceOf[T]
    }

    @inline def getOrNull(index: Int): ScalaType = {
      if (isNull(index)) null.asInstanceOf[ScalaType]
      else get(index)
    }

    // Mutation
    @inline def set(index: Int, value: ScalaType): this.type

    @inline def setOrNull(index: Int, value: ScalaType): this.type = {
      if (value == null) setNull(index)
      else set(index, value)
    }

    @inline def unsafeSet(index: Int, value: Any): this.type = {
      set(index, value.asInstanceOf[ScalaType])
    }

    @inline def setValues(index: Int, values: ArrowArray.Typed[_, ScalaType]): this.type = {
      (0 until values.length).foreach(i => set(index + i, values(i)))

      this
    }

    @inline def setValues(index: Int, values: Array[ScalaType]): this.type = {
      values.zipWithIndex.foreach { case (v, i) => set(index + i, v) }

      this
    }

    @inline def setValues(index: Int, values: Iterable[ScalaType]): this.type = {
      values.zipWithIndex.foreach { case (v, i) => set(index + i, v) }

      this
    }

    @inline def setValues(index: Int, values: Iterator[ScalaType], fetchSize: Int): this.type = {
      values.grouped(size = fetchSize).map(b => setValues(index, b))

      this
    }

    @inline def append(value: ScalaType): this.type = {
      val index = length

      ensureIndex(index)
      set(index, value)
      setValueCount(index + 1)

      this
    }

    @inline def appendValues(values: Iterable[ScalaType]): this.type = {
      val startIndex = length
      val newLength = startIndex + values.size
      ensureIndex(newLength - 1)

      values.zipWithIndex.foreach { case (v, i) =>
        val index = startIndex + i

        set(index, v)
      }

      setValueCount(newLength)

      this
    }

    // Cast
    def as[C : ru.TypeTag]: ArrowArray.Typed[V, C] = {
      val casted = as(ru.typeOf[C].dealias)

      casted.asInstanceOf[ArrowArray.Typed[V, C]]
    }

    def as(tpe: ru.Type): ArrowArray.Typed[V, _] = {
      val st = this.scalaType
      if (this.scalaType =:= tpe) {
        return this
      }

      if (ReflectUtils.isOption(tpe)) {
        val child = ReflectUtils.typeArgument(tpe, 0)

        return as(child).toOptional(tpe)
      }

      this.innerAs(tpe)
    }

    def innerAs(tpe: ru.Type): ArrowArray.Typed[V, _] = {
      throw new IllegalArgumentException(
        s"Cannot cast array from $scalaType to $tpe"
      )
    }

    /**
     * Convert the entire array to a standard Scala array.
     * @return scala.Array[ScalaType]
     */
    def toArray: Array[ScalaType] = toArray(0, length)

    /**
     * Convert a slice of the array to a standard Scala array.
     * @param start start index (inclusive)
     * @param end end index (exclusive)
     * @return scala.Array[ScalaType]
     */
    def toArray(start: Int, end: Int): Array[ScalaType]

    def toOptional(tpe: ru.Type): OptionArray.Typed[V, ScalaType] = {
      new OptionArray.Typed[V, ScalaType](
        scalaType = tpe,
        inner = this
      )
    }

    override def slice(start: Int, end: Int): ArraySlice.Typed[V, ScalaType] = {
      new ArraySlice.Typed[V, ScalaType](
        inner = this,
        startIndex = start,
        endIndex = end
      )
    }
  }

  // Factory methods
  def apply[T](values: T*)(implicit tt: ru.TypeTag[T]): ArrowArray.Typed[_, T] = make(values)

  def empty[T](implicit tt: ru.TypeTag[T]): ArrowArray.Typed[_, T] = make(Seq.empty[T])

  def fill[T](n: Int)(elem: => T)(implicit tt: ru.TypeTag[T]): ArrowArray.Typed[_, T] = {
    val values = Seq.fill(n)(elem)

    make(values)
  }

  def make[T](values: Seq[T])(implicit tt: ru.TypeTag[T]): ArrowArray.Typed[_, T] = {
    val field = ArrowField.fromScala[T]
    val allocator = RootAllocatorExtension.INSTANCE

    // Vector
    val vector = field.createVector(allocator)
    vector.allocateNew()
    vector.setInitialCapacity(values.size)
    vector.setValueCount(values.size)

    // Array
    val array = from(vector).as[T]

    // Fill
    array.setValues(0, values)

    array
  }

  def from(vector: ValueVector): ArrowArray.Typed[_, _] = {
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
}