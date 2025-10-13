package io.github.platob.arrow4s.core.codec.vector

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.codec.convert.ValueConverter
import io.github.platob.arrow4s.core.codec.value.{OptionalValueCodec, ValueCodec}
import io.github.platob.arrow4s.core.codec.value.primitive.PrimitiveValueCodec
import io.github.platob.arrow4s.core.codec.vector.VectorCodec.Typed
import io.github.platob.arrow4s.core.codec.vector.primitive.{ByteBasedVectorCodec, FloatingVectorCodec, IntegralVectorCodec}
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

trait VectorCodec[T] extends Serializable {
  def codec: ValueCodec[T]

  // Mutators
  @inline def isNull(vector: ValueVector, index: Int): Boolean = {
    vector.isNull(index)
  }

  @inline def as[C](implicit codec: ValueConverter[T, C]): Typed[C, _ <: ValueVector]

  def asValue[C](codec: ValueCodec[C]): VectorCodec.Typed[C, _ <: ValueVector]

  def createVector(allocator: BufferAllocator): ValueVector

  def createVector(name: String, allocator: BufferAllocator): ValueVector

  def createVector(field: Field, allocator: BufferAllocator): ValueVector

  def createArray(allocator: BufferAllocator, values: Iterable[T]): ArrowArray[T]
}

object VectorCodec {
  abstract class Typed[T, V <: ValueVector](val codec: ValueCodec[T]) extends VectorCodec[T] {
    override def toString: String = s"${getClass.getSimpleName}[$codec]"

    def toOptional: OptionalVectorCodec.Typed[T, V] = optional[T, V](this, codec.toOptionalCodec)

    @inline def get(vector: V, index: Int): T

    @inline def getOption(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None else Some(get(vector, index))
    }

    // Mutators
    @inline def set(vector: V, index: Int, value: T): Unit

    @inline def setOption(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => set(vector, index, v)
        case None    => setNull(vector, index)
      }
    }

    @inline def setValues(vector: V, index: Int, values: Iterable[T]): Unit = {
      values.zipWithIndex.foreach { case (v, i) => set(vector, index + i, v) }
    }

    @inline def setNull(vector: V, index: Int): Unit

    @inline def append(vector: V, value: T): Unit = {
      val index = vector.getValueCount
      vector.setValueCount(index + 1)
      set(vector, index, value)
    }

    override def as[C](implicit converter: ValueConverter[T, C]): Typed[C, V] = {
      if (this.codec == converter.target) {
        return this.asInstanceOf[Typed[C, V]]
      }

      ConvertVectorCodec.fromConverter[T, C, V](converter, this, converter.target)
    }

    override def asValue[C](codec: ValueCodec[C]): Typed[C, V] = {
      if (codec == this.codec) {
        return this.asInstanceOf[Typed[C, V]]
      }

      val converter = ValueConverter.fromCodecs(this.codec, codec)

      this.as[C](converter)
    }

    override def createVector(allocator: BufferAllocator): V = {
      createVector(codec.arrowField, allocator)
    }

    override def createVector(name: String, allocator: BufferAllocator): V = {
      val field = ArrowField.rename(codec.arrowField, name)


      createVector(field, allocator)
    }

    override def createVector(field: Field, allocator: BufferAllocator): V = {
      val built = field.createVector(allocator).asInstanceOf[V]

      built
    }

    override def createArray(allocator: BufferAllocator, values: Iterable[T]): ArrowArray.Typed[T, V] = {
      val vector = createVector(allocator)
      vector.setInitialCapacity(values.size)
      vector.allocateNew()

      // Set values
      setValues(vector, 0, values)

      // Set value count
      vector.setValueCount(values.size)

      new ArrowArray.Typed[T, V](vector)(this)
    }
  }

  implicit val boolean: IntegralVectorCodec.BooleanVectorCodec = new primitive.IntegralVectorCodec.BooleanVectorCodec
  implicit val byte: IntegralVectorCodec.ByteVectorCodec = new primitive.IntegralVectorCodec.ByteVectorCodec
  implicit val ubyte: IntegralVectorCodec.UByteVectorCodec = new primitive.IntegralVectorCodec.UByteVectorCodec
  implicit val short: IntegralVectorCodec.ShortVectorCodec = new primitive.IntegralVectorCodec.ShortVectorCodec
  implicit val char: IntegralVectorCodec.CharVectorCodec = new primitive.IntegralVectorCodec.CharVectorCodec
  implicit val int: IntegralVectorCodec.IntVectorCodec = new primitive.IntegralVectorCodec.IntVectorCodec
  implicit val uint: IntegralVectorCodec.UIntVectorCodec = new primitive.IntegralVectorCodec.UIntVectorCodec
  implicit val long: IntegralVectorCodec.LongVectorCodec = new primitive.IntegralVectorCodec.LongVectorCodec
  implicit val ulong: IntegralVectorCodec.ULongVectorCodec = new primitive.IntegralVectorCodec.ULongVectorCodec
  implicit val bigInteger: IntegralVectorCodec.BigIntegerVectorCodec = new primitive.IntegralVectorCodec.BigIntegerVectorCodec
  implicit val float: FloatingVectorCodec.FloatVectorCodec = new primitive.FloatingVectorCodec.FloatVectorCodec
  implicit val double: FloatingVectorCodec.DoubleVectorCodec = new primitive.FloatingVectorCodec.DoubleVectorCodec
  implicit val bigDecimal: FloatingVectorCodec.BigDecimalVectorCodec = new primitive.FloatingVectorCodec.BigDecimalVectorCodec
  implicit val binary: ByteBasedVectorCodec.VarBinaryVectorCodec = new primitive.ByteBasedVectorCodec.VarBinaryVectorCodec
  implicit val utf8: ByteBasedVectorCodec.UTF8VectorCodec = new primitive.ByteBasedVectorCodec.UTF8VectorCodec

  implicit def optional[T, V <: ValueVector](implicit
    inner: VectorCodec.Typed[T, V],
    codec: ValueCodec[Option[T]]
  ): OptionalVectorCodec.Typed[T, V] = new OptionalVectorCodec.Typed[T, V](inner)

  def default[T](implicit codec: ValueCodec[T]): VectorCodec.Typed[T, _ <: ValueVector] = {
    codec match {
      case opt: OptionalValueCodec[t] =>
        val built = default[t](opt.inner)

        return built.toOptional.asInstanceOf[VectorCodec.Typed[T, _ <: ValueVector]]
      case _ =>
        // Skip
    }

    codec.arrowTypeId match {
      case ArrowTypeID.Binary =>
        codec match {
          case prim: PrimitiveValueCodec[T] =>
            val converter = ValueConverter.fromBytes[T](prim)

            binary.as[T](converter)
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case ArrowTypeID.Utf8 =>
        codec match {
          case prim: PrimitiveValueCodec[T] =>
            val converter = ValueConverter.fromString[T](prim)

            utf8.as[T](converter)
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case ArrowTypeID.Int =>
        val intType = codec.arrowField.getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Int]

        codec match {
          case prim: PrimitiveValueCodec[T] =>
            intType.getBitWidth match {
              case 8 if intType.getIsSigned  => byte.as[T](ValueConverter.fromByte(prim))
              case 8 if !intType.getIsSigned => ubyte.as[T](ValueConverter.fromUByte(prim))
              case 16 if intType.getIsSigned  => short.as[T](ValueConverter.fromShort(prim))
              case 16 if !intType.getIsSigned => char.as[T](ValueConverter.fromChar(prim))
              case 32 if intType.getIsSigned  => int.as[T](ValueConverter.fromInt(prim))
              case 32 if !intType.getIsSigned => uint.as[T](ValueConverter.fromUInt(prim))
              case 64 if intType.getIsSigned  => long.as[T](ValueConverter.fromLong(prim))
              case 64 if !intType.getIsSigned => ulong.as[T](ValueConverter.fromULong(prim))
              case _ =>
                throw new IllegalArgumentException(s"Unsupported integer type: $intType")
            }
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case ArrowTypeID.FloatingPoint =>
        val floatType = codec.arrowField.getType.asInstanceOf[ArrowType.FloatingPoint]

        codec match {
          case prim: PrimitiveValueCodec[T] =>
            floatType.getPrecision match {
              case FloatingPointPrecision.SINGLE => float.as[T](ValueConverter.fromFloat(prim))
              case FloatingPointPrecision.DOUBLE => double.as[T](ValueConverter.fromDouble(prim))
              case _ =>
                throw new IllegalArgumentException(s"Unsupported floating point type: $floatType")
            }
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case ArrowTypeID.Decimal =>
        val decimalType = codec.arrowField.getType.asInstanceOf[ArrowType.Decimal]

        codec match {
          case prim: PrimitiveValueCodec[T] =>
            decimalType.getBitWidth match {
              case 128 => bigDecimal.as[T](ValueConverter.fromBigDecimal(prim))
            }
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case ArrowTypeID.Bool =>
        codec match {
          case prim: PrimitiveValueCodec[T] =>
            boolean.as[T](ValueConverter.fromBoolean(prim))
          case _ =>
            throw new IllegalArgumentException(s"No default VectorCodec for codec with type: $codec")
        }
      case _ =>
        throw new IllegalArgumentException(s"No default VectorCodec for codec with type: ${codec.arrowTypeId}")
    }
  }
}
