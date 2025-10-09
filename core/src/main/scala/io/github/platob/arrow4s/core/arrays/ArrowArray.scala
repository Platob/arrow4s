package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.nested.StructArray
import io.github.platob.arrow4s.core.arrays.primitive.{BinaryArray, FloatingPointArray, IntegralArray}
import io.github.platob.arrow4s.core.arrays.traits.TArrowArray
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.extensions.RootAllocatorExtension
import io.github.platob.arrow4s.core.values.ValueConverter
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.collection.{Factory, mutable}

trait ArrowArray[Value] extends TArrowArray
  with scala.collection.immutable.IndexedSeq[Value] {
  def codec: ValueCodec[Value]

  def children: Seq[ArrowArray[_]]

  def childFields: Seq[Field] = children.map(_.field)

  @inline private def childIndex(name: String): Int = {
    val index = childFields.indexWhere(_.getName == name)

    if (index == -1) {
      // Try case-insensitive match
      return childFields.indexWhere(_.getName.equalsIgnoreCase(name))
    }

    index
  }

  @throws[NoSuchElementException]
  @inline def childAt(index: Int): ArrowArray[_] = {
    try {
      children(index)
    } catch {
      case _: IndexOutOfBoundsException =>
        throw new NoSuchElementException(s"No child at index $index within ${childFields.map(_.getName).mkString("['", "', '", "']")}")
    }
  }

  @throws[NoSuchElementException]
  @inline def child(name: String): ArrowArray[_] = {
    val index = childIndex(name)

    try {
      children(index)
    } catch {
      case _: IndexOutOfBoundsException =>
        throw new NoSuchElementException(s"No child with name '$name' within ${childFields.map(_.getName).mkString("['", "', '", "']")}")
    }
  }

  // Accessors
  @inline def apply(index: Int): Value = getOrDefault(index)

  @inline def get(index: Int): Value

  override def unsafeGet[V](index: Int): V = {
    get(index).asInstanceOf[V]
  }

  @inline def getOrDefault(index: Int): Value = {
    if (isNull(index)) this.codec.default
    else get(index)
  }

  // Mutation
  @inline def set(index: Int, value: Value): this.type

  override def unsafeSet(index: Int, value: Any): Unit = {
    set(index, value.asInstanceOf[Value])
  }

  @inline def setValues(index: Int, values: Iterable[Value]): this.type = {
    values.zipWithIndex.foreach { case (v, i) => set(index + i, v) }

    this
  }

  @inline def append(value: Value): this.type = {
    val index = length

    ensureIndex(index)
    set(index, value)
    setValueCount(index + 1)

    this
  }

  @inline def appendValues(values: Iterable[Value]): this.type = {
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


  /**
   * Convert the entire array to a standard Scala array.
   * @return scala.Array[ScalaType]
   */
  def toArray: Array[Value] = toArray(0, length)

  /**
   * Convert a slice of the array to a standard Scala array.
   * @param start start index (inclusive)
   * @param end end index (exclusive)
   * @return scala.Array[ScalaType]
   */
  def toArray(start: Int, end: Int): Array[Value] = {
    (start until end).map(get).toArray(this.codec.clsTag)
  }

  // Cast
  def as[C](implicit codec: ValueCodec[C], converter: ValueConverter[Value, C]): ArrowArray[C]

  def asUnsafe[C](implicit codec: ValueCodec[C]): ArrowArray[C] = {
    val casted = asUnchecked(codec)

    casted.asInstanceOf[ArrowArray[C]]
  }

  private[core] def asUnchecked(codec: ValueCodec[_]): ArrowArray[_] = {
    if (this.codec == codec) {
      return this
    }

    val result = if (codec.isOption) {
      val casted = asUnchecked(codec = codec.childAt(0))

      casted.toOptional.asInstanceOf[ArrowArray[_]]
    } else {
      this.innerAs(codec)
    }

    result
  }

  @inline def toOptional: LogicalArray.Optional[Value, _, _]

  private[core] def innerAs(codec: ValueCodec[_]): ArrowArray[_]

  def arrowSlice(start: Int, end: Int): ArraySlice[Value]
}

object ArrowArray {
  trait Typed[Value, ArrowVector <: ValueVector, Arr <: Typed[Value, ArrowVector, Arr]]
    extends ArrowArray[Value] {
    def vector: ArrowVector

    // Cast
    def as[C](implicit codec: ValueCodec[C], converter: ValueConverter[Value, C]): ArrowArray.Typed[C, ArrowVector, _] = {
      if (this.codec == codec) {
        return this.asInstanceOf[ArrowArray.Typed[C, ArrowVector, _]]
      }

      LogicalArray.converter[Value, ArrowVector, Arr, C](
        array = this.asInstanceOf[Arr],
        codec = codec
      )(converter = converter)
    }

    def toOptional: LogicalArray.Optional[Value, ArrowVector, Arr] = {
      LogicalArray.optional[Value, ArrowVector, Arr](this.asInstanceOf[Arr], this.codec.toOptionalCodec)
    }

    override def arrowSlice(start: Int, end: Int): ArraySlice.Typed[Value, ArrowVector, Arr] = {
      new ArraySlice.Typed[Value, ArrowVector, Arr](
        inner = this.asInstanceOf[Arr],
        startIndex = start,
        endIndex = end
      )
    }
  }

  // Factory methods
  def apply[T](values: T*)(implicit codec: ValueCodec[T]): ArrowArray[T] = make(values)

  def empty[T](implicit codec: ValueCodec[T]): ArrowArray[T] = make(Seq.empty[T])

  def fill[T](n: Int)(elem: => T)(implicit codec: ValueCodec[T]): ArrowArray[T] = {
    val values = Seq.fill(n)(elem)

    make(values)
  }

  def make[T](values: Seq[T])(implicit codec: ValueCodec[T]): ArrowArray[T] = {
    val field = codec.arrowField
    val allocator = RootAllocatorExtension.INSTANCE

    // Vector
    val vector = field.createVector(allocator)
    vector.allocateNew()
    vector.setInitialCapacity(values.size)
    vector.setValueCount(values.size)

    // Array
    val array = from(vector).asUnsafe[T]

    // Fill
    array.setValues(0, values)

    array
  }

  def from(vector: ValueVector): ArrowArray.Typed[_, _, _] = {
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

  // Produces a concrete typed ArrowArray for the element type T
  implicit def arrowArrayFactory[T](implicit codec: ValueCodec[T]): Factory[T, ArrowArray[T]] =
    new Factory[T, ArrowArray[T]] {
      def fromSpecific(it: IterableOnce[T]): ArrowArray[T] = {
        val b = newBuilder
        b.addAll(it)
        b.result()
      }

      def newBuilder: mutable.Builder[T, ArrowArray[T]] =
        new mutable.Builder[T, ArrowArray[T]] {
          // Allocate an Arrow vector using the codec's Arrow field
          private val field     = codec.arrowField
          private val allocator = RootAllocatorExtension.INSTANCE
          private val vector    = {
            val v = field.createVector(allocator)
            v.allocateNew()
            v
          }

          // Wrap it as an ArrowArray and write into it incrementally
          private val arr = ArrowArray.from(vector).asUnsafe[T]
          private var i   = 0

          def addOne(x: T): this.type = {
            arr.append(x)
            i += 1
            this
          }

          def clear(): Unit = {
            vector.setValueCount(0)
            i = 0
          }

          def result(): ArrowArray[T] = {
            arr.setValueCount(i)
            arr
          }
        }
    }
}