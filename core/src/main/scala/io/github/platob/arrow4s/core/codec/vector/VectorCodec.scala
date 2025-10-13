package io.github.platob.arrow4s.core.codec.vector

import io.github.platob.arrow4s.core.codec.convert.ValueConverter
import io.github.platob.arrow4s.core.codec.value.ValueCodec
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

  @inline def unsafeGet(vector: ValueVector, index: Int): T

  // Mutators
  @inline def unsafeSet(vector: ValueVector, index: Int, value: T): Unit

  @inline def unsafeSetNull(vector: ValueVector, index: Int): Unit

  @inline def isNull(vector: ValueVector, index: Int): Boolean = {
    vector.isNull(index)
  }

  @inline def as[C](implicit converter: ValueConverter[T, C]): Typed[C, _ <: ValueVector]

  def asValue[C](codec: ValueCodec[C]): VectorCodec.Typed[C, _ <: ValueVector]
}

object VectorCodec {
  abstract class Typed[T, V <: ValueVector](val codec: ValueCodec[T]) extends VectorCodec[T] {
    override def toString: String = s"${getClass.getSimpleName}[$codec]"

    def toOptional: OptionalVectorCodec.Typed[T, V] = optional[T, V](this, codec.toOptionalCodec)

    override def unsafeGet(vector: ValueVector, index: Int): T = {
      get(vector.asInstanceOf[V], index)
    }

    @inline def get(vector: V, index: Int): T

    @inline def getOption(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None else Some(get(vector, index))
    }

    // Mutators
    override def unsafeSet(vector: ValueVector, index: Int, value: T): Unit = {
      set(vector.asInstanceOf[V], index, value)
    }

    @inline def set(vector: V, index: Int, value: T): Unit

    @inline def setOption(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => set(vector, index, v)
        case None    => setNull(vector, index)
      }
    }

    @inline def setNull(vector: V, index: Int): Unit

    override def unsafeSetNull(vector: ValueVector, index: Int): Unit = {
      setNull(vector.asInstanceOf[V], index)
    }

    override def as[C](implicit converter: ValueConverter[T, C]): Typed[C, V] = {
      if (this.codec == converter.target) {
        return this.asInstanceOf[Typed[C, V]]
      }

      new ConvertVectorCodec.Typed[T, C, V]()(this, converter)
    }

    override def asValue[C](codec: ValueCodec[C]): Typed[C, V] = {
      if (codec == this.codec) {
        return this.asInstanceOf[Typed[C, V]]
      }

      val converter = ValueConverter.fromCodecs(this.codec, codec)

      this.as[C](converter)
    }

    def createVector(allocator: BufferAllocator): V = {
      createVector(codec.arrowField, allocator)
    }

    def createVector(name: String, allocator: BufferAllocator): V = {
      val field = ArrowField.rename(codec.arrowField, name)


      createVector(field, allocator)
    }

    def createVector(field: Field, allocator: BufferAllocator): V = {
      val built = field.createVector(allocator).asInstanceOf[V]

      built
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
    codec.arrowTypeId match {
      case ArrowTypeID.Binary =>
        binary.as[T]
      case ArrowTypeID.Utf8 =>
        utf8.as[T]
      case ArrowTypeID.Int =>
        val intType = codec.arrowField.getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Int]

        (intType.getBitWidth, intType.getIsSigned) match {
          case (8, true) =>
            byte.as[T]
          case (8, false) =>
            ubyte.as[T]
          case (16, true) =>
            short.as[T]
          case (16, false) =>
            char.as[T]
          case (32, true) =>
            int.as[T]
          case (32, false) =>
            uint.as[T]
          case (64, true) =>
            long.as[T]
          case (64, false) =>
            ulong.as[T]
          case _ =>
            throw new IllegalArgumentException(s"Unsupported integer type: $intType")
        }
      case ArrowTypeID.FloatingPoint =>
        val floatType = codec.arrowField.getType.asInstanceOf[ArrowType.FloatingPoint]

        floatType.getPrecision match {
          case FloatingPointPrecision.SINGLE =>
            float.as[T]
          case FloatingPointPrecision.DOUBLE =>
            double.as[T]
          case _ =>
            throw new IllegalArgumentException(s"Unsupported floating point type: $floatType")
        }
      case ArrowTypeID.Decimal =>
        val decimalType = codec.arrowField.getType.asInstanceOf[ArrowType.Decimal]

        decimalType.getBitWidth match {
          case 128 =>
            bigDecimal.as[T]
//          case 256 =>
//            new Typed[T, Decimal256Vector](bigDecimal.codec)
          case _ =>
            throw new IllegalArgumentException(s"Unsupported decimal type: $decimalType")
        }
      case ArrowTypeID.Bool =>
        boolean.as[T]
      case _ =>
        throw new IllegalArgumentException(s"No default VectorCodec for codec with type: ${codec.arrowTypeId}")
    }
  }
}
