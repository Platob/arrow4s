package io.github.platob.arrow4s.core
package arrays

import io.github.platob.arrow4s.core.arrays.traits.TArrowArray
import io.github.platob.arrow4s.core.codec.convert.ValueConverter
import io.github.platob.arrow4s.core.codec.value.ValueCodec
import io.github.platob.arrow4s.core.codec.vector.VectorCodec
import io.github.platob.arrow4s.core.extensions.RootAllocatorExtension
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID

trait ArrowArray[T] extends TArrowArray
  with scala.collection.immutable.IndexedSeq[T] {
  override def codec: VectorCodec[T]

  // Accessors
  @inline def getObject(index: Int): T

  // Mutation
  @inline def apply(index: Int, value: T): Unit = setObject(index, value)

  @inline def setObject(index: Int, value: T): this.type

  @inline def setValues(index: Int, values: Iterable[T]): this.type

  @inline def append(value: T): this.type

  @inline def :+(value: T): this.type = append(value)

  @inline def appendValues(values: Iterable[T]): this.type = {
    values.foreach(append)
    this
  }

  @inline def ++(values: Iterable[T]): this.type = appendValues(values)

  /**
   * Convert the entire array to a standard Scala array.
   * @return scala.Array[ScalaType]
   */
  def toArray: Array[T] = toArray(0, length)

  /**
   * Convert a slice of the array to a standard Scala array.
   * @param start start index (inclusive)
   * @param end end index (exclusive)
   * @return scala.Array[ScalaType]
   */
  def toArray(start: Int, end: Int): Array[T] = {
    (start until end).map(getObject).toArray(this.codec.codec.clsTag)
  }

  def arrowSlice(start: Int, end: Int): ArraySlice[T]

  def as[C](implicit converter: ValueConverter[T, C]): ArrowArray[C]

  def asCodec[C](implicit cast: ValueCodec[C]): ArrowArray[C]
}

object ArrowArray {
  class Typed[
    Value, ArrowVector <: ValueVector
  ](val vector: ArrowVector)(implicit val codec: VectorCodec.Typed[Value, ArrowVector])
    extends ArrowArray[Value] {
    override def apply(index: Int): Value = getObject(index)

    override def getObject(index: Int): Value = codec.get(vector, index)

    override def setObject(index: Int, value: Value): this.type = {
      codec.set(vector, index, value)
      this
    }

    override def setValues(index: Int, values: Iterable[Value]): Typed.this.type = {
      codec.setValues(vector, index, values)
      this
    }

    override def append(value: Value): Typed.this.type = {
      codec.append(vector, value)
      this
    }

    def as[C](implicit cast: VectorCodec.Typed[C, ArrowVector]): Typed[C, ArrowVector] = {
      if (this.codec.codec == cast.codec)
        return this.asInstanceOf[ArrowArray.Typed[C, ArrowVector]]

      new ArrowArray.Typed[C, ArrowVector](vector)(cast)
    }

    override def as[C](implicit converter: ValueConverter[Value, C]): ArrowArray[C] = {
      if (converter.target == this.codec.codec)
        return this.asInstanceOf[ArrowArray[C]]

      val vectorCodec = this.codec.as[C]

      this.as[C](vectorCodec)
    }

    override def asCodec[C](implicit cast: ValueCodec[C]): ArrowArray[C] = {
      if (this.codec.codec == cast)
        return this.asInstanceOf[ArrowArray[C]]

      val vectorCodec = this.codec.asValue[C](cast)

      this.as[C](vectorCodec)
    }

    override def arrowSlice(start: Int, end: Int): ArraySlice.Typed[Value, ArrowVector] = {
      new ArraySlice.Typed[Value, ArrowVector](
        inner = this,
        startIndex = start,
        endIndex = end
      )
    }
  }

  // Factory methods
  def apply[T](values: T*)(implicit codec: ValueCodec[T]): ArrowArray[T] = make(values)

  def empty[T](implicit codec: ValueCodec[T]): ArrowArray[T] = make(Seq.empty[T])

  def make[T](values: Seq[T])(implicit codec: ValueCodec[T]): ArrowArray[T] = {
    VectorCodec.default[T].createArray(
      allocator = RootAllocatorExtension.INSTANCE,
      values = values
    )
  }

  def default(vector: ValueVector): ArrowArray.Typed[_, _] = {
    val field = vector.getField
    val dtype = field.getType

    dtype.getTypeID match {
      case ArrowTypeID.Binary =>
        new Typed[Array[Byte], VarBinaryVector](vector.asInstanceOf[VarBinaryVector])
      case ArrowTypeID.Utf8 =>
        new Typed[String, VarCharVector](vector.asInstanceOf[VarCharVector])
      case ArrowTypeID.Int =>
        val arrowType = dtype.asInstanceOf[ArrowType.Int]

        (arrowType.getBitWidth, arrowType.getIsSigned) match {
          case (8, true) =>
            new Typed[Byte, TinyIntVector](vector.asInstanceOf[TinyIntVector])
          case (8, false) =>
            new Typed[UByte, UInt1Vector](vector.asInstanceOf[UInt1Vector])
          case (16, true) =>
            new Typed[Short, SmallIntVector](vector.asInstanceOf[SmallIntVector])
          case (16, false) =>
            new Typed[Char, UInt2Vector](vector.asInstanceOf[UInt2Vector])
          case (32, true) =>
            new Typed[Int, IntVector](vector.asInstanceOf[IntVector])
          case (32, false) =>
            new Typed[UInt, UInt4Vector](vector.asInstanceOf[UInt4Vector])
          case (64, true) =>
            new Typed[Long, BigIntVector](vector.asInstanceOf[BigIntVector])
          case (64, false) =>
            new Typed[ULong, UInt8Vector](vector.asInstanceOf[UInt8Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported integer type: $arrowType")
        }
      case ArrowTypeID.FloatingPoint =>
        val arrowType = dtype.asInstanceOf[ArrowType.FloatingPoint]

        arrowType.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            new Typed[Float, Float4Vector](vector.asInstanceOf[Float4Vector])
          case FloatingPointPrecision.DOUBLE =>
            new Typed[Double, Float8Vector](vector.asInstanceOf[Float8Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported floating point type: $arrowType")
        }
      case ArrowTypeID.Decimal =>
        val arrowType = dtype.asInstanceOf[ArrowType.Decimal]

        arrowType.getBitWidth match {
          case 128 =>
            new Typed[java.math.BigDecimal, DecimalVector](vector.asInstanceOf[DecimalVector])
//          case 256 =>
//            new FloatingPointArray.Decimal256Array(vector.asInstanceOf[Decimal256Vector])
          case _ =>
            throw new IllegalArgumentException(s"Unsupported decimal type: $arrowType")
        }
      case ArrowTypeID.Bool =>
        new Typed[Boolean, BitVector](vector.asInstanceOf[BitVector])
//      case ArrowTypeID.Struct =>
//        StructArray.default(vector.asInstanceOf[StructVector])
//      case ArrowTypeID.List =>
//        ListArray.default(vector.asInstanceOf[ListVector])
//      case ArrowTypeID.Map =>
//        MapArray.default(vector.asInstanceOf[MapVector])
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: $dtype")
    }
  }
}