package io.github.platob.arrow4s.core.codec

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.codec.nested._
import io.github.platob.arrow4s.core.codec.primitive.{NumericCodec, PrimitiveCodec}
import io.github.platob.arrow4s.core.types.ArrowField
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, ValueConverter}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.nio.charset.Charset
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

trait ValueCodec[T] {
  override def toString: String = s"${getClass.getSimpleName}[$namespace]"

  def namespace: String

  /**
   * Name of this type (e.g., Int, String, MyCaseClass).
   */
  def arrowField: Field

  def arrowTypeId: ArrowTypeID = arrowField.getType.getTypeID

  def toOptionalCodec: OptionalCodec[T] = OptionalCodec.make[T](this)

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

  def isOption: Boolean = tpe.typeConstructor =:= ru.typeOf[Option[_]].typeConstructor

  def isTuple: Boolean = tpe.typeSymbol.fullName.startsWith("scala.Tuple")

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

  def toValue[P](value: T)(implicit converter: ValueConverter[T, P]): P = converter.decode(value)
  def fromValue[P](value: P)(implicit converter: ValueConverter[P, T]): T = converter.decode(value)

  // Convenience common types
  @inline def toBytes(value: T): Array[Byte]
  @inline def fromBytes(value: Array[Byte]): T

  @inline def toString(value: T): String = toString(value, ValueCodec.UTF8_CHARSET)
  @inline def fromString(value: String): T = fromString(value, ValueCodec.UTF8_CHARSET)

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

  // Arrow Array
  @inline def arrowGet(array: ArrowArray[T], index: Int): T = array.get(index)

  @inline def arrowSet(array: ArrowArray[T], index: Int, value: T): Unit = array.set(index, value)
}

object ValueCodec {
  private val UTF8_CHARSET: Charset = Charset.forName("UTF-8")

  implicit val bit: PrimitiveCodec[Boolean] = new NumericCodec[Boolean] {
    override val bitSize: Int = 1
    override val arrowField: Field = ArrowField.build(
      name = "Boolean",
      at = ArrowType.Bool.INSTANCE,
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Boolean): Array[Byte] = Array(if (value) 1.toByte else 0.toByte)
    override def fromBytes(value: Array[Byte]): Boolean = {
      require(value.nonEmpty, s"Expected 1 byte or more, got ${value.length}")
      value(0) != byte.zero
    }

    override def toString(value: Boolean, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Boolean = value.head match {
      case 't' | 'T' | '1' | 'y' | 'Y' => true
      case 'f' | 'F' | '0' | 'n' | 'N' => false
      case other => throw new IllegalArgumentException(s"Cannot parse Boolean from string starting with '$other'")
    }

    override def toBoolean(value: Boolean): Boolean = value
    override def fromBoolean(value: Boolean): Boolean = value

    override def toInt(value: Boolean): Int = if (value) 1 else 0
    override def fromInt(value: Int): Boolean = value != 0

    override def toLong(value: Boolean): Long = if (value) 1L else 0L
    override def fromLong(value: Long): Boolean = value != 0L

    override def toBigInteger(value: Boolean): java.math.BigInteger =
      if (value) java.math.BigInteger.ONE else java.math.BigInteger.ZERO
    override def fromBigInteger(value: java.math.BigInteger): Boolean =
      value != java.math.BigInteger.ZERO

    override def toDouble(value: Boolean): Double = if (value) 1.0 else 0.0
    override def fromDouble(value: Double): Boolean = value != 0.0

    override def toBigDecimal(value: Boolean): java.math.BigDecimal =
      if (value) java.math.BigDecimal.ONE else java.math.BigDecimal.ZERO
    override def fromBigDecimal(value: java.math.BigDecimal): Boolean =
      value != java.math.BigDecimal.ZERO
  }

  implicit val byte: PrimitiveCodec[Byte] = new NumericCodec[Byte] {
    override val bitSize: Int = 8
    override val arrowField: Field = ArrowField.build(
      name = "Byte",
      at = new ArrowType.Int(8, true),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Byte): Array[Byte] = Array(value)
    override def fromBytes(value: Array[Byte]): Byte = {
      require(value.nonEmpty, s"Expected 1 byte or more, got ${value.length}")
      value(0)
    }

    override def toString(value: Byte, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Byte = value.toByte

    override def toInt(value: Byte): Int = value.toInt
    override def fromInt(value: Int): Byte = value.toByte

    override def toLong(value: Byte): Long = value.toLong
    override def fromLong(value: Long): Byte = value.toByte

    override def toBigInteger(value: Byte): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Byte = value.byteValue()

    override def toDouble(value: Byte): Double = value.toDouble
    override def fromDouble(value: Double): Byte = value.toByte

    override def toBigDecimal(value: Byte): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Byte = value.byteValue()
  }

  implicit val ubyte: PrimitiveCodec[UByte] = new NumericCodec[UByte] {
    override def bitSize: Int = arrowField.getType.asInstanceOf[ArrowType.Int].getBitWidth
    override val arrowField: Field = ArrowField.build(
      name = "UByte",
      at = new ArrowType.Int(8, false),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: UByte): Array[Byte] = byte.toBytes(value.toByte)
    override def fromBytes(value: Array[Byte]): UByte = {
      UByte.unsafe(byte.fromBytes(value))
    }

    override def toString(value: UByte, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): UByte = UByte.trunc(value.toInt)

    override def toInt(value: UByte): Int = value.toInt
    override def fromInt(value: Int): UByte = UByte.trunc(value)

    override def toLong(value: UByte): Long = value.toLong
    override def fromLong(value: Long): UByte = UByte.trunc(value)

    override def toBigInteger(value: UByte): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): UByte = UByte.trunc(value.intValue())

    override def toDouble(value: UByte): Double = value.toDouble
    override def fromDouble(value: Double): UByte = UByte.trunc(value.toInt)

    override def toBigDecimal(value: UByte): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): UByte = UByte.trunc(value.intValue())
  }

  implicit val short: PrimitiveCodec[Short] = new NumericCodec[Short] {
    override val bitSize: Int = 16
    override val arrowField: Field = ArrowField.build(
      name = "Short",
      at = new ArrowType.Int(16, true),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Short): Array[Byte] = {
      Array(
        (value & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
      )
    }

    override def fromBytes(value: Array[Byte]): Short = {
      require(value.length > 1, s"Expected 2 bytes or more, got ${value.length}")

      ((value(0) & 0xFF) | ((value(1) & 0xFF) << 8)).toShort
    }

    override def toString(value: Short, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Short = value.toShort

    override def toInt(value: Short): Int = value.toInt
    override def fromInt(value: Int): Short = value.toShort

    override def toLong(value: Short): Long = value.toLong
    override def fromLong(value: Long): Short = value.toShort

    override def toBigInteger(value: Short): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Short = value.shortValue()

    override def toDouble(value: Short): Double = value.toDouble
    override def fromDouble(value: Double): Short = value.toShort

    override def toBigDecimal(value: Short): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Short = value.shortValue()
  }

  implicit val char: PrimitiveCodec[Char] = new NumericCodec[Char] {
    override val bitSize: Int = 16
    override val arrowField: Field = ArrowField.build(
      name = "Char",
      at = new ArrowType.Int(16, false),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Char): Array[Byte] = short.toBytes(value.toShort)
    override def fromBytes(value: Array[Byte]): Char = {
      short.fromBytes(value).toChar
    }

    override def toString(value: Char, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Char = {
      require(value.length == 1, s"Cannot parse Char from string of length ${value.length}")
      value.charAt(0)
    }

    override def toChar(value: Char): Char = value
    override def fromChar(value: Char): Char = value

    override def toInt(value: Char): Int = value.toInt
    override def fromInt(value: Int): Char = value.toChar

    override def toLong(value: Char): Long = value.toLong
    override def fromLong(value: Long): Char = value.toChar

    override def toBigInteger(value: Char): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Char = value.intValue().toChar

    override def toDouble(value: Char): Double = value.toDouble
    override def fromDouble(value: Double): Char = value.toChar

    override def toBigDecimal(value: Char): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Char = value.intValue().toChar
  }

  implicit val int: PrimitiveCodec[Int] = new NumericCodec[Int] {
    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = "Int",
      at = new ArrowType.Int(32, true),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Int): Array[Byte] = {
      Array(
        (value & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 24) & 0xFF).toByte,
      )
    }

    override def fromBytes(value: Array[Byte]): Int = {
      require(value.length > 3, s"Expected 4 bytes or more, got ${value.length}")

      (value(0) & 0xFF) |
        ((value(1) & 0xFF) << 8) |
        ((value(2) & 0xFF) << 16) |
        ((value(3) & 0xFF) << 24)
    }

    override def toString(value: Int, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Int = value.toInt

    override def toInt(value: Int): Int = value
    override def fromInt(value: Int): Int = value

    override def toLong(value: Int): Long = value.toLong
    override def fromLong(value: Long): Int = value.toInt

    override def toBigInteger(value: Int): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): Int = value.intValue()

    override def toDouble(value: Int): Double = value.toDouble
    override def fromDouble(value: Double): Int = value.toInt

    override def toBigDecimal(value: Int): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): Int = value.intValue()
  }

  implicit val uint: PrimitiveCodec[UInt] = new NumericCodec[UInt] {
    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = "UInt",
      at = new ArrowType.Int(32, false),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: UInt): Array[Byte] = int.toBytes(value.toInt)
    override def fromBytes(value: Array[Byte]): UInt = {
      UInt.unsafe(int.fromBytes(value))
    }

    override def toString(value: UInt, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): UInt = UInt.trunc(value.toLong)

    override def toInt(value: UInt): Int = value.toInt
    override def fromInt(value: Int): UInt = UInt.trunc(value)

    override def toUInt(value: UInt): UInt = value
    override def fromUInt(value: UInt): UInt = value

    override def toLong(value: UInt): Long = value.toLong
    override def fromLong(value: Long): UInt = UInt.trunc(value)

    override def toBigInteger(value: UInt): java.math.BigInteger = java.math.BigInteger.valueOf(value.toLong)
    override def fromBigInteger(value: java.math.BigInteger): UInt = UInt.trunc(value.intValue())

    override def toDouble(value: UInt): Double = value.toDouble
    override def fromDouble(value: Double): UInt = UInt.trunc(value.toLong)

    override def toBigDecimal(value: UInt): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toLong)
    override def fromBigDecimal(value: java.math.BigDecimal): UInt = UInt.trunc(value.longValue())
  }

  implicit val long: PrimitiveCodec[Long] = new NumericCodec[Long] {
    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = "Long",
      at = new ArrowType.Int(64, true),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Long): Array[Byte] = {
      Array(
        (value & 0xFF).toByte,
        ((value >> 8) & 0xFF).toByte,
        ((value >> 16) & 0xFF).toByte,
        ((value >> 24) & 0xFF).toByte,
        ((value >> 32) & 0xFF).toByte,
        ((value >> 40) & 0xFF).toByte,
        ((value >> 48) & 0xFF).toByte,
        ((value >> 56) & 0xFF).toByte,
      )
    }

    override def fromBytes(value: Array[Byte]): Long = {
      require(value.length > 7, s"Expected 8 bytes or more, got ${value.length}")

      (value(0).toLong & 0xFFL) |
        ((value(1).toLong & 0xFFL) << 8) |
        ((value(2).toLong & 0xFFL) << 16) |
        ((value(3).toLong & 0xFFL) << 24) |
        ((value(4).toLong & 0xFFL) << 32) |
        ((value(5).toLong & 0xFFL) << 40) |
        ((value(6).toLong & 0xFFL) << 48) |
        ((value(7).toLong & 0xFFL) << 56)
    }

    override def toString(value: Long, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Long = value.toLong

    override def toInt(value: Long): Int = value.toInt
    override def fromInt(value: Int): Long = value.toLong

    override def toLong(value: Long): Long = value
    override def fromLong(value: Long): Long = value

    override def toBigInteger(value: Long): java.math.BigInteger = java.math.BigInteger.valueOf(value)
    override def fromBigInteger(value: java.math.BigInteger): Long = value.longValue()

    override def toDouble(value: Long): Double = value.toDouble
    override def fromDouble(value: Double): Long = value.toLong

    override def toBigDecimal(value: Long): java.math.BigDecimal = java.math.BigDecimal.valueOf(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Long = value.longValue()
  }

  implicit val ulong: PrimitiveCodec[ULong] = new NumericCodec[ULong] {
    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = "ULong",
      at = new ArrowType.Int(64, false),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: ULong): Array[Byte] = long.toBytes(value.toLong)
    override def fromBytes(value: Array[Byte]): ULong = {
      ULong.unsafe(long.fromBytes(value))
    }

    override def toString(value: ULong, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): ULong = ULong.trunc(long.fromString(value))

    override def toInt(value: ULong): Int = value.toInt
    override def fromInt(value: Int): ULong = ULong.trunc(value)

    override def toUInt(value: ULong): UInt = UInt.trunc(value.toInt)
    override def fromUInt(value: UInt): ULong = ULong.trunc(value.toLong)

    override def toLong(value: ULong): Long = value.toLong
    override def fromLong(value: Long): ULong = ULong.trunc(value)

    override def toBigInteger(value: ULong): java.math.BigInteger = value.toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): ULong = ULong.trunc(value)

    override def toDouble(value: ULong): Double = value.toDouble
    override def fromDouble(value: Double): ULong = ULong.trunc(value.toLong)

    override def toBigDecimal(value: ULong): java.math.BigDecimal = value.toBigDecimal
    override def fromBigDecimal(value: java.math.BigDecimal): ULong = ULong.trunc(value.longValue())
  }

  implicit val float: PrimitiveCodec[Float] = new NumericCodec[Float] {
    override val bitSize: Int = 32
    override val arrowField: Field = ArrowField.build(
      name = "Float",
      at = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: Float): Array[Byte] = int.toBytes(java.lang.Float.floatToIntBits(value))
    override def fromBytes(value: Array[Byte]): Float = java.lang.Float.intBitsToFloat(int.fromBytes(value))

    override def toString(value: Float, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Float = value.toFloat

    override def toInt(value: Float): Int = value.toInt
    override def fromInt(value: Int): Float = value.toFloat

    override def toLong(value: Float): Long = value.toLong
    override def fromLong(value: Long): Float = value.toFloat

    override def toBigInteger(value: Float): java.math.BigInteger = java.math.BigDecimal.valueOf(value.toDouble).toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): Float = value.floatValue()

    override def toDouble(value: Float): Double = value.toDouble
    override def fromDouble(value: Double): Float = value.toFloat

    override def toBigDecimal(value: Float): java.math.BigDecimal = java.math.BigDecimal.valueOf(value.toDouble)
    override def fromBigDecimal(value: java.math.BigDecimal): Float = value.floatValue()
  }

  implicit val double: PrimitiveCodec[Double] = new NumericCodec[Double] {
    override val bitSize: Int = 64
    override val arrowField: Field = ArrowField.build(
      name = "Double",
      at = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      nullable = false,
      metadata = None,
      children = Nil
    )
    override def toBytes(value: Double): Array[Byte] = long.toBytes(java.lang.Double.doubleToLongBits(value))
    override def fromBytes(value: Array[Byte]): Double = java.lang.Double.longBitsToDouble(long.fromBytes(value))

    override def toString(value: Double, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): Double = value.toDouble

    override def toInt(value: Double): Int = value.toInt
    override def fromInt(value: Int): Double = value.toDouble

    override def toLong(value: Double): Long = value.toLong
    override def fromLong(value: Long): Double = value.toDouble

    override def toBigInteger(value: Double): java.math.BigInteger = java.math.BigDecimal.valueOf(value).toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): Double = value.doubleValue()

    override def toDouble(value: Double): Double = value
    override def fromDouble(value: Double): Double = value

    override def toBigDecimal(value: Double): java.math.BigDecimal = java.math.BigDecimal.valueOf(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Double = value.doubleValue()
  }

  implicit val bigInteger: PrimitiveCodec[java.math.BigInteger] = new NumericCodec[java.math.BigInteger] {
    override val bitSize: Int = 128 // variable size
    override val arrowField: Field = ArrowField.build(
      name = "BigInteger",
      at = new ArrowType.Decimal(38, 0, 128),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: java.math.BigInteger): Array[Byte] = value.toByteArray
    override def fromBytes(value: Array[Byte]): java.math.BigInteger = new java.math.BigInteger(value)

    override def toString(value: java.math.BigInteger, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): java.math.BigInteger = new java.math.BigInteger(value)

    override def toBoolean(value: java.math.BigInteger): Boolean = bit.fromBigInteger(value)
    override def fromBoolean(value: Boolean): java.math.BigInteger = bit.toBigInteger(value)

    override def toInt(value: java.math.BigInteger): Int = int.fromBigInteger(value)
    override def fromInt(value: Int): java.math.BigInteger = int.toBigInteger(value)

    override def toLong(value: java.math.BigInteger): Long = long.fromBigInteger(value)
    override def fromLong(value: Long): java.math.BigInteger = long.toBigInteger(value)

    override def toBigInteger(value: java.math.BigInteger): java.math.BigInteger = value
    override def fromBigInteger(value: java.math.BigInteger): java.math.BigInteger = value

    override def toDouble(value: java.math.BigInteger): Double = double.fromBigInteger(value)
    override def fromDouble(value: Double): java.math.BigInteger = double.toBigInteger(value)

    override def toBigDecimal(value: java.math.BigInteger): java.math.BigDecimal = new java.math.BigDecimal(value)
    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigInteger = value.toBigInteger
  }

  implicit val bigDecimal: PrimitiveCodec[java.math.BigDecimal] = new NumericCodec[java.math.BigDecimal] {
    override val bitSize: Int = 128 // variable size
    override val arrowField: Field = ArrowField.build(
      name = "BigDecimal",
      at = new ArrowType.Decimal(38, 10, 128),
      nullable = false,
      metadata = None,
      children = Nil
    )

    override def toBytes(value: java.math.BigDecimal): Array[Byte] = value.unscaledValue().toByteArray
    override def fromBytes(value: Array[Byte]): java.math.BigDecimal = new java.math.BigDecimal(new java.math.BigInteger(value))

    override def toString(value: java.math.BigDecimal, charset: Charset): String = value.toString
    override def fromString(value: String, charset: Charset): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toBoolean(value: java.math.BigDecimal): Boolean = bit.fromBigDecimal(value)
    override def fromBoolean(value: Boolean): java.math.BigDecimal = bit.toBigDecimal(value)

    override def toInt(value: java.math.BigDecimal): Int = int.fromBigDecimal(value)
    override def fromInt(value: Int): java.math.BigDecimal = int.toBigDecimal(value)

    override def toLong(value: java.math.BigDecimal): Long = long.fromBigDecimal(value)
    override def fromLong(value: Long): java.math.BigDecimal = long.toBigDecimal(value)

    override def toBigInteger(value: java.math.BigDecimal): java.math.BigInteger = value.toBigInteger
    override def fromBigInteger(value: java.math.BigInteger): java.math.BigDecimal = new java.math.BigDecimal(value)

    override def toDouble(value: java.math.BigDecimal): Double = double.fromBigDecimal(value)
    override def fromDouble(value: Double): java.math.BigDecimal = double.toBigDecimal(value)

    override def toBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal = value
    override def fromBigDecimal(value: java.math.BigDecimal): java.math.BigDecimal = value
  }

  implicit val bytes: PrimitiveCodec[Array[Byte]] = new PrimitiveCodec[Array[Byte]] {
    override val bitSize: Int = -1 // variable size
    override val arrowField: Field = ArrowField.build(
      name = "Bytes",
      at = ArrowType.Binary.INSTANCE,
      nullable = false,
      metadata = None,
      children = Nil
    )

    override val zero: Array[Byte] = Array.empty
    override val one: Array[Byte] = Array(1.toByte)

    override def toBytes(value: Array[Byte]): Array[Byte] = value
    override def fromBytes(value: Array[Byte]): Array[Byte] = value

    override def toString(value: Array[Byte], charset: Charset): String = new String(value, charset)
    override def fromString(value: String, charset: Charset): Array[Byte] = value.getBytes(charset)

    override def toBoolean(value: Array[Byte]): Boolean = bit.fromBytes(value)
    override def fromBoolean(value: Boolean): Array[Byte] = bit.toBytes(value)

    override def toByte(value: Array[Byte]): Byte = byte.fromBytes(value)
    override def fromByte(value: Byte): Array[Byte] = byte.toBytes(value)

    override def toShort(value: Array[Byte]): Short = short.fromBytes(value)
    override def fromShort(value: Short): Array[Byte] = short.toBytes(value)

    override def toChar(value: Array[Byte]): Char = char.fromBytes(value)
    override def fromChar(value: Char): Array[Byte] = char.toBytes(value)

    override def toInt(value: Array[Byte]): Int = int.fromBytes(value)
    override def fromInt(value: Int): Array[Byte] = int.toBytes(value)

    override def toLong(value: Array[Byte]): Long = long.fromBytes(value)
    override def fromLong(value: Long): Array[Byte] = long.toBytes(value)

    override def toBigInteger(value: Array[Byte]): java.math.BigInteger = bigInteger.fromBytes(value)
    override def fromBigInteger(value: java.math.BigInteger): Array[Byte] = bigInteger.toBytes(value)

    override def toFloat(value: Array[Byte]): Float = float.fromBytes(value)
    override def fromFloat(value: Float): Array[Byte] = float.toBytes(value)

    override def toDouble(value: Array[Byte]): Double = double.fromBytes(value)
    override def fromDouble(value: Double): Array[Byte] = double.toBytes(value)

    override def toBigDecimal(value: Array[Byte]): java.math.BigDecimal = bigDecimal.fromBytes(value)
    override def fromBigDecimal(value: java.math.BigDecimal): Array[Byte] = bigDecimal.toBytes(value)
  }

  implicit val string: PrimitiveCodec[String] = new PrimitiveCodec[String] {
    override val bitSize: Int = -1 // variable size
    override val arrowField: Field = ArrowField.build(
      name = "String",
      at = ArrowType.Utf8.INSTANCE,
      nullable = false,
      metadata = None,
      children = Nil
    )

    override val zero: String = ""
    override val one: String = "1"

    override def toBytes(value: String): Array[Byte] = value.getBytes("UTF-8")
    override def fromBytes(value: Array[Byte]): String = new String(value, "UTF-8")

    override def toString(value: String, charset: Charset): String = value
    override def fromString(value: String, charset: Charset): String = value

    override def toBoolean(value: String): Boolean = bit.fromString(value)
    override def fromBoolean(value: Boolean): String = bit.toString(value)

    override def toByte(value: String): Byte = byte.fromString(value)
    override def fromByte(value: Byte): String = byte.toString(value)

    override def toShort(value: String): Short = short.fromString(value)
    override def fromShort(value: Short): String = short.toString(value)

    override def toChar(value: String): Char = char.fromString(value)
    override def fromChar(value: Char): String = char.toString(value)

    override def toInt(value: String): Int = int.fromString(value)
    override def fromInt(value: Int): String = int.toString(value)

    override def toLong(value: String): Long = long.fromString(value)
    override def fromLong(value: Long): String = long.toString(value)

    override def toBigInteger(value: String): java.math.BigInteger = bigInteger.fromString(value)
    override def fromBigInteger(value: java.math.BigInteger): String = bigInteger.toString(value)

    override def toFloat(value: String): Float = float.fromString(value)
    override def fromFloat(value: Float): String = float.toString(value)

    override def toDouble(value: String): Double = double.fromString(value)
    override def fromDouble(value: Double): String = double.toString(value)

    override def toBigDecimal(value: String): java.math.BigDecimal = bigDecimal.fromString(value)
    override def fromBigDecimal(value: java.math.BigDecimal): String = bigDecimal.toString(value)
  }

  def fromField(arrowField: Field): ValueCodec[_] = {
    val dtype = arrowField.getFieldType
    val arrowType = dtype.getType

    arrowType.getTypeID match {
      case ArrowTypeID.Bool =>
        bit
      case ArrowTypeID.Binary | ArrowTypeID.LargeBinary =>
        bytes
      case ArrowTypeID.Utf8 | ArrowTypeID.LargeUtf8 =>
        string
      case ArrowTypeID.Int =>
        val intType = arrowType.asInstanceOf[ArrowType.Int]

        if (intType.getIsSigned) {
          intType.getBitWidth match {
            case 8  => byte
            case 16 => short
            case 32 => int
            case 64 => long
            case other => throw new IllegalArgumentException(s"Unsupported Int bit width: $other")
          }
        } else {
          intType.getBitWidth match {
            case 8  => ubyte
            case 16 => char
            case 32 => uint
            case 64 => ulong
            case other => throw new IllegalArgumentException(s"Unsupported UInt bit width: $other")
          }
        }
      case ArrowTypeID.FloatingPoint =>
        val floatType = arrowType.asInstanceOf[ArrowType.FloatingPoint]

        floatType.getPrecision match {
          case FloatingPointPrecision.SINGLE => float
          case FloatingPointPrecision.DOUBLE => double
          case other => throw new IllegalArgumentException(s"Unsupported FloatingPoint precision: $other")
        }
      case ArrowTypeID.Decimal =>
        val decType = arrowType.asInstanceOf[ArrowType.Decimal]
        if (decType.getScale == 0) bigInteger
        else bigDecimal
      case ArrowTypeID.List =>
        val childField = arrowField.getChildren.get(0)
        val childCodec = fromField(childField)

        childCodec match {
          case c: ValueCodec[t] @unchecked =>
            ListCodec.arrowArray[t](c)
        }
      case ArrowTypeID.Map =>
        val entryField = arrowField.getChildren.get(0)
        val entryCodec = StructCodec.fromField(entryField)

        entryCodec match {
          case c: Tuple2Codec[k, v] @unchecked =>
            MapCodec.mapFromPair[k, v](c)
          case _ =>
            throw new IllegalArgumentException(s"Map entry field is not a Tuple2: $entryCodec")
        }
      case ArrowTypeID.Struct =>
        StructCodec.fromField(arrowField)
    }
  }


  // Pre-register builtin codecs (add more if you define them later)
  locally {
    ValueCodecRegistry.register(bit); ValueCodecRegistry.register(byte); ValueCodecRegistry.register(ubyte); ValueCodecRegistry.register(short); ValueCodecRegistry.register(char)
    ValueCodecRegistry.register(int); ValueCodecRegistry.register(uint); ValueCodecRegistry.register(long); ValueCodecRegistry.register(ulong)
    ValueCodecRegistry.register(float); ValueCodecRegistry.register(double); ValueCodecRegistry.register(bigInteger); ValueCodecRegistry.register(bigDecimal)
    ValueCodecRegistry.register(bytes); ValueCodecRegistry.register(string)
    // Optional is higher-order; it’s created on demand below
  }

  // ===== implicit builders (put inside `object ValueCodec`) =====

  // --- Optional ---
  implicit def optionalCodec[T](implicit vc: ValueCodec[T]): OptionalCodec[T] =
    OptionalCodec.make[T]

  implicit def mapCodec[K, V](implicit
    pair: Tuple2Codec[K, V],
    tt: ru.TypeTag[Map[K, V]],
    ct: ClassTag[Map[K, V]]
  ): MapCodec[K, V, Map] = MapCodec.map[K, V]

  implicit def sepCodec[T](implicit
    vc: ValueCodec[T],
    tt: ru.TypeTag[Seq[T]],
    ct: ClassTag[Seq[T]],
  ): ListCodec[T, Seq] = ListCodec.seq[T]

  implicit def listCodec[T](implicit
    vc: ValueCodec[T],
    tt: ru.TypeTag[List[T]],
    ct: ClassTag[List[T]],
  ): ListCodec[T, List] = ListCodec.list[T]

  // --- Product / Tuple fallback (low priority-ish) ---
  implicit def productCodec[T <: Product](implicit tt: ru.TypeTag[T], ct: ClassTag[T]): ValueCodec[T] =
    StructCodec.product[T]
}
