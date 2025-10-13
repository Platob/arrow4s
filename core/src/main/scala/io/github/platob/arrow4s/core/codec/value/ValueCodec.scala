package io.github.platob.arrow4s.core.codec.value

import io.github.platob.arrow4s.core.codec.convert.ValueConverter
import io.github.platob.arrow4s.core.codec.value.nested.TupleValueCodec.Tuple2ValueCodec
import io.github.platob.arrow4s.core.codec.value.nested.{ListValueCodec, MapValueCodec, StructValueCodec, TupleValueCodec}
import io.github.platob.arrow4s.core.codec.value.primitive.ByteBasedValueCodec.{BinaryValueCodec, StringValueCodec}
import io.github.platob.arrow4s.core.codec.value.primitive.FloatingValueCodec.{BigDecimalValueCodec, DoubleValueCodec, FloatValueCodec}
import io.github.platob.arrow4s.core.codec.value.primitive.IntegralValueCodec._
import io.github.platob.arrow4s.core.codec.value.primitive.{ByteBasedValueCodec, FloatingValueCodec, IntegralValueCodec}
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.nio.charset.Charset
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait ValueCodec[T] extends Serializable {
  override def toString: String = s"${getClass.getSimpleName}[$namespace]"

  def namespace: String

  def isPrimitive: Boolean

  def isOption: Boolean = tpe.typeConstructor =:= ru.typeOf[Option[_]].typeConstructor

  def isTuple: Boolean

  /**
   * Name of this type (e.g., Int, String, MyCaseClass).
   */
  def arrowField: Field

  def arrowTypeId: ArrowTypeID = arrowField.getType.getTypeID

  def toOptionalCodec: OptionalValueCodec[T] = OptionalValueCodec.make[T](this)

  /**
   * Scala reflection Type for this codec's type.
   * @return runtime.Type
   */
  def tpe: ru.Type

  /**
   * TypeTag for compile-time type information (e.g., for generics).
   */
  def typeTag: ru.TypeTag[T]

  /**
   * ClassTag for runtime type information (e.g., for arrays).
   */
  def clsTag: ClassTag[T]

  // Children
  def children: Seq[ValueCodec[_]]

  def childAt(index: Int): ValueCodec[_] = children(index)

  def arity: Int = children.length

  def arrowChildren: Seq[FieldCodec[_]] = this.children
    .zip(this.arrowField.getChildren.asScala)
    .map {
      case (c, f) => FieldCodec(field = f, codec = c)
    }

  // Values
  /**
   * Size in bits of this type. Use -1 for variable-size types (e.g., String, List).
   */
  def bitSize: Int

  def fixedSize: Boolean = bitSize > 0

  /**
   * A zero value for this type (e.g., 0 for Int, "" for String, case class with all fields set to zero).
   * @return the zero value
   */
  def zero: T

  /**
   * A one value for this type (e.g., 1 for Int, "\u0001" for String, case class with all fields set to one).
   * @return the one value
   */
  def one: T

  /**
   * A default value for this type (e.g., 0 for Int, "" for String, case class with all fields set to default).
   * @return the default value
   */
  def default: T

  /**
   * Access the element at the given index for composite types (e.g., case classes, tuples).
   * @param value the value to access
   */
  @inline def elementAt[Elem](value: T, index: Int): Elem

  /**
   * Convert the value into an array of its elements.
   * @param value the value to convert
   * @return
   */
  @inline def elements(value: T): Array[Any] = {
    (0 until arity).map(i => elementAt[Any](value, i)).toArray
  }

  /**
   * Create a value from an array of its elements.
   * @param values the elements to create the value from
   * @return
   */
  @inline def fromElements(values: Array[Any]): T

  @inline def unsafeAs(value: Any): T = value.asInstanceOf[T]

  def isNull(value: T): Boolean = value == null

  def toValue[P](value: T)(implicit converter: ValueConverter[T, P]): P = converter.encode(value)
  def fromValue[P](value: P)(implicit converter: ValueConverter[P, T]): T = converter.encode(value)

  // Convenience common types
  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toString(value: T): String = toString(value, ByteBasedValueCodec.UTF8_CHARSET)
  @inline def fromString(value: String): T = fromString(value, ByteBasedValueCodec.UTF8_CHARSET)

  @inline def toString(value: T, charset: Charset): String
  @inline def fromString(value: String, charset: Charset): T

  @inline def toBoolean(value: T): Boolean
  @inline def fromBoolean(value: Boolean): T

  @inline def toByte(value: T): Byte = toInt(value).toByte
  @inline def fromByte(value: Byte): T = fromInt(value.toInt)

  @inline def toUByte(value: T): UByte = UByte.trunc(toInt(value))
  @inline def fromUByte(value: UByte): T = fromInt(value.toInt)

  @inline def toShort(value: T): Short = toInt(value).toShort
  @inline def fromShort(value: Short): T = fromInt(value.toInt)

  @inline def toChar(value: T): Char = toInt(value).toChar
  @inline def fromChar(value: Char): T = fromInt(value.toInt)

  @inline def toInt(value: T): Int
  @inline def fromInt(value: Int): T

  @inline def toUInt(value: T): UInt = UInt.trunc(toLong(value))
  @inline def fromUInt(value: UInt): T = fromLong(value.toInt)

  @inline def toLong(value: T): Long
  @inline def fromLong(value: Long): T

  @inline def toULong(value: T): ULong = ULong.trunc(toBigInteger(value))
  @inline def fromULong(value: ULong): T = fromBigInteger(value.toBigInteger)

  @inline def toBigInteger(value: T): java.math.BigInteger
  @inline def fromBigInteger(value: java.math.BigInteger): T

  @inline def toFloat(value: T): Float = toDouble(value).toFloat
  @inline def fromFloat(value: Float): T = fromDouble(value.toDouble)

  @inline def toDouble(value: T): Double
  @inline def fromDouble(value: Double): T

  @inline def toBigDecimal(value: T): java.math.BigDecimal
  @inline def fromBigDecimal(value: java.math.BigDecimal): T
}

object ValueCodec {
  def fromField(arrowField: Field): ValueCodec[_] = {
    val dtype = arrowField.getFieldType
    val arrowType = dtype.getType

    arrowType.getTypeID match {
      case ArrowTypeID.Bool =>
        boolean
      case ArrowTypeID.Binary | ArrowTypeID.LargeBinary =>
        binary
      case ArrowTypeID.Utf8 | ArrowTypeID.LargeUtf8 =>
        utf8
      case ArrowTypeID.Int =>
        val intType = arrowType.asInstanceOf[ArrowType.Int]

        IntegralValueCodec.fromArrowType(intType)
      case ArrowTypeID.FloatingPoint =>
        FloatingValueCodec.fromArrowType(arrowType.asInstanceOf[ArrowType.FloatingPoint])
      case ArrowTypeID.Decimal =>
        FloatingValueCodec.fromArrowType(arrowType.asInstanceOf[ArrowType.Decimal])
      case ArrowTypeID.List =>
        val childField = arrowField.getChildren.get(0)
        val childCodec = fromField(childField)

        childCodec match {
          case c: ValueCodec[t] @unchecked =>
            ListValueCodec.arrowArray[t](c)
        }
      case ArrowTypeID.Map =>
        val entryField = arrowField.getChildren.get(0)
        val entryCodec = TupleValueCodec.tupleFromField(entryField)

        entryCodec match {
          case c: Tuple2ValueCodec[k, v] @unchecked =>
            MapValueCodec.mapFromPair[k, v](c)
          case _ =>
            throw new IllegalArgumentException(s"Map entry field is not a Tuple2: $entryCodec")
        }
//      case ArrowTypeID.Struct =>
//        StructValueCodec.fromField(arrowField)
    }
  }

  implicit val boolean: BooleanValueCodec = new BooleanValueCodec
  implicit val byte: ByteValueCodec = new ByteValueCodec
  implicit val ubyte: UByteValueCodec = new UByteValueCodec
  implicit val short: ShortValueCodec = new ShortValueCodec
  implicit val char: CharValueCodec = new CharValueCodec
  implicit val int: IntValueCodec = new IntValueCodec
  implicit val uint: UIntValueCodec = new UIntValueCodec
  implicit val long: LongValueCodec = new LongValueCodec
  implicit val ulong: ULongValueCodec = new ULongValueCodec
  implicit val bigInteger: BigIntegerValueCodec = new BigIntegerValueCodec
  implicit val float: FloatValueCodec = new FloatValueCodec
  implicit val double: DoubleValueCodec = new DoubleValueCodec
  implicit val bigDecimal: BigDecimalValueCodec = new BigDecimalValueCodec
  implicit val binary: BinaryValueCodec = new BinaryValueCodec
  implicit val utf8: StringValueCodec = new StringValueCodec


  // Pre-register builtin codecs (add more if you define them later)
  locally {
    ValueCodecRegistry.register(boolean); ValueCodecRegistry.register(byte); ValueCodecRegistry.register(ubyte); ValueCodecRegistry.register(short); ValueCodecRegistry.register(char)
    ValueCodecRegistry.register(int); ValueCodecRegistry.register(uint); ValueCodecRegistry.register(long); ValueCodecRegistry.register(ulong)
    ValueCodecRegistry.register(float); ValueCodecRegistry.register(double); ValueCodecRegistry.register(bigInteger); ValueCodecRegistry.register(bigDecimal)
    ValueCodecRegistry.register(binary); ValueCodecRegistry.register(utf8)
    // Optional is higher-order; it’s created on demand below
  }

  // ===== implicit builders (put inside `object ValueCodec`) =====

  // --- Optional ---
  implicit def optionalCodec[T](implicit vc: ValueCodec[T]): OptionalValueCodec[T] =
    OptionalValueCodec.make[T]

  implicit def mapCodec[K, V](implicit
    pair: Tuple2ValueCodec[K, V],
    tt: ru.TypeTag[Map[K, V]],
    ct: ClassTag[Map[K, V]]
  ): MapValueCodec[K, V, Map] = MapValueCodec.map[K, V]

  implicit def sepCodec[T](implicit
    vc: ValueCodec[T],
    tt: ru.TypeTag[Seq[T]],
    ct: ClassTag[Seq[T]],
  ): ListValueCodec[T, Seq] = ListValueCodec.seq[T]

  implicit def listCodec[T](implicit
    vc: ValueCodec[T],
    tt: ru.TypeTag[List[T]],
    ct: ClassTag[List[T]],
  ): ListValueCodec[T, List] = ListValueCodec.list[T]

  // --- Product / Tuple fallback (low priority-ish) ---
  implicit def productCodec[T <: Product](implicit tt: ru.TypeTag[T], ct: ClassTag[T]): ValueCodec[T] =
    StructValueCodec.product[T]
}
