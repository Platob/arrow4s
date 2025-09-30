package io.github.platob.arrow4s.core.cast

import io.github.platob.arrow4s.core.reflection.ReflectUtils
import io.github.platob.arrow4s.core.values.{UByte, UInt, ULong, UShort}

import scala.reflect.runtime.{universe => ru}

sealed abstract class TypeConverter[Source : ru.TypeTag, Target : ru.TypeTag] {
  override def toString: String = s"TypeConverter[${this.sourceType}, ${this.targetType}]"

  def sourceType: ru.Type = ru.typeOf[Source].dealias

  def targetType: ru.Type = ru.typeOf[Target].dealias

  def key: (ru.Type, ru.Type) = (sourceType, targetType)

  def to: Source => Target

  def from: Target => Source

  def reverse: TypeConverter[Target, Source] = TypeConverter.instance(from, to)

  def optional: TypeConverter[Option[Source], Option[Target]] =
    TypeConverter.instance(optS => optS.map(to), optT => optT.map(from))

  def optionalTarget: TypeConverter[Source, Option[Target]] =
    TypeConverter.instance(s => Some(to(s)), {
      case Some(t) => from(t)
      case None    => throw new NoSuchElementException("Cannot convert None to Target")
    })

  def optionalSource: TypeConverter[Option[Source], Target] =
    TypeConverter.instance({
      case Some(s) => to(s)
      case None    => throw new NoSuchElementException("Cannot convert None to Source")
    }, t => Some(from(t)))

  def cast(castTo: ru.Type): TypeConverter[Source, _] = {
    if (castTo =:= targetType)
      this
    else
      TypeConverter.get(sourceType, castTo).asInstanceOf[TypeConverter[Source, _]]
  }
}

object TypeConverter {
  // Byte
  implicit val byteToByte: TypeConverter[Byte, Byte] = identity
  implicit val byteToUByte: TypeConverter[Byte, UByte] = instance(b => UByte.trunc(b), u => u.toByte)
  implicit val byteToShort: TypeConverter[Byte, Short] = instance(b => b.toShort, s => s.toByte)
  implicit val byteToUShort: TypeConverter[Byte, UShort] = instance(b => UShort.trunc(b), u => u.toByte)
  implicit val byteToInt: TypeConverter[Byte, Int] = instance(b => b.toInt, i => i.toByte)
  implicit val byteToUInt: TypeConverter[Byte, UInt] = instance(b => UInt.trunc(b), u => u.toByte)
  implicit val byteToLong: TypeConverter[Byte, Long] = instance(b => b.toLong, l => l.toByte)
  implicit val byteToULong: TypeConverter[Byte, ULong] = instance(b => ULong.trunc(b), u => u.toByte)
  implicit val byteToFloat: TypeConverter[Byte, Float] = instance(b => b.toFloat, f => f.toByte)
  implicit val byteToDouble: TypeConverter[Byte, Double] = instance(b => b.toDouble, d => d.toByte)
  implicit val byteToString: TypeConverter[Byte, String] = instance(b => b.toString, s => s.toByte)
  implicit val byteToBoolean: TypeConverter[Byte, Boolean] = instance(b => b != 0, bool => if (bool) 1 else 0)
  implicit val byteToBigInteger: TypeConverter[Byte, java.math.BigInteger] =
    instance(b => java.math.BigInteger.valueOf(b.toLong), bi => bi.byteValueExact())
  implicit val byteToBigDecimal: TypeConverter[Byte, java.math.BigDecimal] =
    instance(b => java.math.BigDecimal.valueOf(b.toLong), bd => bd.byteValueExact())
  implicit val byteToBytes: TypeConverter[Byte, Array[Byte]] = instance(b => Array(b), arr => arr.head)

  // UByte
  implicit val ubyteToByte: TypeConverter[UByte, Byte] = byteToUByte.reverse
  implicit val ubyteToUByte: TypeConverter[UByte, UByte] = identity
  implicit val ubyteToShort: TypeConverter[UByte, Short] = instance(u => u.toShort, s => UByte.trunc(s))
  implicit val ubyteToUShort: TypeConverter[UByte, UShort] = instance(u => UShort.trunc(u.toInt), us => UByte.trunc(us.toInt))
  implicit val ubyteToInt: TypeConverter[UByte, Int] = instance(u => u.toInt, i => UByte.trunc(i))
  implicit val ubyteToUInt: TypeConverter[UByte, UInt] = instance(u => UInt.trunc(u.toInt), ui => UByte.trunc(ui.toInt))
  implicit val ubyteToLong: TypeConverter[UByte, Long] = instance(u => u.toLong, l => UByte.trunc(l))
  implicit val ubyteToULong: TypeConverter[UByte, ULong] = instance(u => ULong.trunc(u.toLong), ul => UByte.trunc(ul.toLong))
  implicit val ubyteToFloat: TypeConverter[UByte, Float] = instance(u => u.toFloat, f => UByte.trunc(f.toInt))
  implicit val ubyteToDouble: TypeConverter[UByte, Double] = instance(u => u.toDouble, d => UByte.trunc(d.toInt))
  implicit val ubyteToString: TypeConverter[UByte, String] = instance(u => u.toString, s => UByte.trunc(s.toInt))
  implicit val ubyteToBoolean: TypeConverter[UByte, Boolean] = instance(u => u.toInt != 0, bool => if (bool) UByte(1) else UByte(0))
  implicit val ubyteToBigInteger: TypeConverter[UByte, java.math.BigInteger] =
    instance(u => java.math.BigInteger.valueOf(u.toLong), bi => UByte.trunc(bi.byteValueExact()))
  implicit val ubyteToBigDecimal: TypeConverter[UByte, java.math.BigDecimal] =
    instance(u => java.math.BigDecimal.valueOf(u.toLong), bd => UByte.trunc(bd.byteValueExact()))
  implicit val ubyteToBytes: TypeConverter[UByte, Array[Byte]] = instance(u => Array(u.toByte), arr => UByte.trunc(arr.head))

  // Short
  implicit val shortToByte: TypeConverter[Short, Byte] = byteToShort.reverse
  implicit val shortToUByte: TypeConverter[Short, UByte] = ubyteToShort.reverse
  implicit val shortToShort: TypeConverter[Short, Short] = identity
  implicit val shortToUShort: TypeConverter[Short, UShort] = instance(s => UShort.trunc(s), us => us.toShort)
  implicit val shortToInt: TypeConverter[Short, Int] = instance(s => s.toInt, i => i.toShort)
  implicit val shortToUInt: TypeConverter[Short, UInt] = instance(s => UInt.trunc(s), ui => ui.toShort)
  implicit val shortToLong: TypeConverter[Short, Long] = instance(s => s.toLong, l => l.toShort)
  implicit val shortToULong: TypeConverter[Short, ULong] = instance(s => ULong.trunc(s), ul => ul.toShort)
  implicit val shortToFloat: TypeConverter[Short, Float] = instance(s => s.toFloat, f => f.toShort)
  implicit val shortToDouble: TypeConverter[Short, Double] = instance(s => s.toDouble, d => d.toShort)
  implicit val shortToString: TypeConverter[Short, String] = instance(s => s.toString, str => str.toShort)
  implicit val shortToBoolean: TypeConverter[Short, Boolean] = instance(s => s != 0, bool => if (bool) 1 else 0)
  implicit val shortToBigInteger: TypeConverter[Short, java.math.BigInteger] =
    instance(s => java.math.BigInteger.valueOf(s.toLong), bi => bi.shortValueExact())
  implicit val shortToBigDecimal: TypeConverter[Short, java.math.BigDecimal] =
    instance(s => java.math.BigDecimal.valueOf(s.toLong), bd => bd.shortValueExact())
  implicit val shortToBytes: TypeConverter[Short, Array[Byte]] = instance(s => Array((s & 0xFF).toByte, ((s >> 8) & 0xFF).toByte), arr => {
    if (arr.length != 2) throw new IllegalArgumentException("Array length must be 2 to convert to Short")
    ((arr(1) & 0xFF) << 8 | (arr(0) & 0xFF)).toShort
  })

  // UShort
  implicit val ushortToByte: TypeConverter[UShort, Byte] = byteToUShort.reverse
  implicit val ushortToUByte: TypeConverter[UShort, UByte] = ubyteToUShort.reverse
  implicit val ushortToShort: TypeConverter[UShort, Short] = shortToUShort.reverse
  implicit val ushortToUShort: TypeConverter[UShort, UShort] = identity
  implicit val ushortToInt: TypeConverter[UShort, Int] = instance(us => us.toInt, i => UShort.trunc(i))
  implicit val ushortToUInt: TypeConverter[UShort, UInt] = instance(us => UInt.trunc(us.toInt), ui => UShort.trunc(ui.toInt))
  implicit val ushortToLong: TypeConverter[UShort, Long] = instance(us => us.toLong, l => UShort.trunc(l))
  implicit val ushortToULong: TypeConverter[UShort, ULong] = instance(us => ULong.trunc(us.toLong), ul => UShort.trunc(ul.toLong))
  implicit val ushortToFloat: TypeConverter[UShort, Float] = instance(us => us.toFloat, f => UShort.trunc(f.toInt))
  implicit val ushortToDouble: TypeConverter[UShort, Double] = instance(us => us.toDouble, d => UShort.trunc(d.toInt))
  implicit val ushortToString: TypeConverter[UShort, String] = instance(us => us.toString, s => UShort.trunc(s.toInt))
  implicit val ushortToBoolean: TypeConverter[UShort, Boolean] = instance(us => us.toInt != 0, bool => if (bool) UShort(1) else UShort(0))
  implicit val ushortToBigInteger: TypeConverter[UShort, java.math.BigInteger] =
    instance(us => java.math.BigInteger.valueOf(us.toLong), bi => UShort.trunc(bi.shortValueExact()))
  implicit val ushortToBigDecimal: TypeConverter[UShort, java.math.BigDecimal] =
    instance(us => java.math.BigDecimal.valueOf(us.toLong), bd => UShort.trunc(bd.shortValueExact()))
  implicit val ushortToBytes: TypeConverter[UShort, Array[Byte]] = instance(
    us => shortToBytes.to(us.toShort),
    arr => UShort.trunc(shortToBytes.from(arr))
  )

  // Int
  implicit val intToByte: TypeConverter[Int, Byte] = byteToInt.reverse
  implicit val intToUByte: TypeConverter[Int, UByte] = ubyteToInt.reverse
  implicit val intToShort: TypeConverter[Int, Short] = shortToInt.reverse
  implicit val intToUShort: TypeConverter[Int, UShort] = ushortToInt.reverse
  implicit val intToInt: TypeConverter[Int, Int] = identity
  implicit val intToUInt: TypeConverter[Int, UInt] = instance(i => UInt.trunc(i), ui => ui.toInt)
  implicit val intToLong: TypeConverter[Int, Long] = instance(i => i.toLong, l => l.toInt)
  implicit val intToULong: TypeConverter[Int, ULong] = instance(i => ULong.trunc(i), ul => ul.toInt)
  implicit val intToFloat: TypeConverter[Int, Float] = instance(i => i.toFloat, f => f.toInt)
  implicit val intToDouble: TypeConverter[Int, Double] = instance(i => i.toDouble, d => d.toInt)
  implicit val intToString: TypeConverter[Int, String] = instance(i => i.toString, s => s.toInt)
  implicit val intToBoolean: TypeConverter[Int, Boolean] = instance(i => i != 0, bool => if (bool) 1 else 0)
  implicit val intToBigInteger: TypeConverter[Int, java.math.BigInteger] =
    instance(i => java.math.BigInteger.valueOf(i.toLong), bi => bi.intValueExact())
  implicit val intToBigDecimal: TypeConverter[Int, java.math.BigDecimal] =
    instance(i => java.math.BigDecimal.valueOf(i.toLong), bd => bd.intValueExact())
  implicit val intToBytes: TypeConverter[Int, Array[Byte]] = instance(i => Array(
    (i & 0xFF).toByte,
    ((i >> 8) & 0xFF).toByte,
    ((i >> 16) & 0xFF).toByte,
    ((i >> 24) & 0xFF).toByte
  ), arr => {
    if (arr.length != 4) throw new IllegalArgumentException("Array length must be 4 to convert to Int")
    (arr(0) & 0xFF) |
    ((arr(1) & 0xFF) << 8) |
    ((arr(2) & 0xFF) << 16) |
    ((arr(3) & 0xFF) << 24)
  })

  // UInt
  implicit val uintToByte: TypeConverter[UInt, Byte] = byteToUInt.reverse
  implicit val uintToUByte: TypeConverter[UInt, UByte] = ubyteToUInt.reverse
  implicit val uintToShort: TypeConverter[UInt, Short] = shortToUInt.reverse
  implicit val uintToUShort: TypeConverter[UInt, UShort] = ushortToUInt.reverse
  implicit val uintToInt: TypeConverter[UInt, Int] = intToUInt.reverse
  implicit val uintToUInt: TypeConverter[UInt, UInt] = identity
  implicit val uintToLong: TypeConverter[UInt, Long] = instance(ui => ui.toLong, l => UInt.trunc(l))
  implicit val uintToULong: TypeConverter[UInt, ULong] = instance(ui => ULong.trunc(ui.toLong), ul => UInt.trunc(ul.toLong))
  implicit val uintToFloat: TypeConverter[UInt, Float] = instance(ui => ui.toFloat, f => UInt.trunc(f.toLong))
  implicit val uintToDouble: TypeConverter[UInt, Double] = instance(ui => ui.toDouble, d => UInt.trunc(d.toLong))
  implicit val uintToString: TypeConverter[UInt, String] = instance(ui => ui.toString, s => UInt.trunc(s.toLong))
  implicit val uintToBoolean: TypeConverter[UInt, Boolean] = instance(ui => ui.toInt != 0, bool => if (bool) UInt.One else UInt.Zero)
  implicit val uintToBigInteger: TypeConverter[UInt, java.math.BigInteger] =
    instance(ui => java.math.BigInteger.valueOf(ui.toLong), bi => UInt.trunc(bi.intValueExact()))
  implicit val uintToBigDecimal: TypeConverter[UInt, java.math.BigDecimal] =
    instance(ui => java.math.BigDecimal.valueOf(ui.toLong), bd => UInt.trunc(bd.intValueExact()))
  implicit val uintToBytes: TypeConverter[UInt, Array[Byte]] = instance(
    ui => intToBytes.to(ui.toInt),
    arr => UInt.trunc(intToBytes.from(arr))
  )

  // Long
  implicit val longToByte: TypeConverter[Long, Byte] = byteToLong.reverse
  implicit val longToUByte: TypeConverter[Long, UByte] = ubyteToLong.reverse
  implicit val longToShort: TypeConverter[Long, Short] = shortToLong.reverse
  implicit val longToUShort: TypeConverter[Long, UShort] = ushortToLong.reverse
  implicit val longToInt: TypeConverter[Long, Int] = intToLong.reverse
  implicit val longToUInt: TypeConverter[Long, UInt] = uintToLong.reverse
  implicit val longToLong: TypeConverter[Long, Long] = identity
  implicit val longToULong: TypeConverter[Long, ULong] = instance(l => ULong.trunc(l), ul => ul.toLong)
  implicit val longToFloat: TypeConverter[Long, Float] = instance(l => l.toFloat, f => f.toLong)
  implicit val longToDouble: TypeConverter[Long, Double] = instance(l => l.toDouble, d => d.toLong)
  implicit val longToString: TypeConverter[Long, String] = instance(l => l.toString, s => s.toLong)
  implicit val longToBoolean: TypeConverter[Long, Boolean] = instance(l => l != 0, bool => if (bool) 1L else 0L)
  implicit val longToBigInteger: TypeConverter[Long, java.math.BigInteger] =
    instance(l => java.math.BigInteger.valueOf(l), bi => bi.longValueExact())
  implicit val longToBigDecimal: TypeConverter[Long, java.math.BigDecimal] =
    instance(l => java.math.BigDecimal.valueOf(l), bd => bd.longValueExact())
  implicit val longToBytes: TypeConverter[Long, Array[Byte]] = instance(l => Array(
    (l & 0xFF).toByte,
    ((l >> 8) & 0xFF).toByte,
    ((l >> 16) & 0xFF).toByte,
    ((l >> 24) & 0xFF).toByte,
    ((l >> 32) & 0xFF).toByte,
    ((l >> 40) & 0xFF).toByte,
    ((l >> 48) & 0xFF).toByte,
    ((l >> 56) & 0xFF).toByte
  ), arr => {
    if (arr.length != 8) throw new IllegalArgumentException("Array length must be 8 to convert to Long")
    (arr(0).toLong & 0xFF) |
    ((arr(1).toLong & 0xFF) << 8) |
    ((arr(2).toLong & 0xFF) << 16) |
    ((arr(3).toLong & 0xFF) << 24) |
    ((arr(4).toLong & 0xFF) << 32) |
    ((arr(5).toLong & 0xFF) << 40) |
    ((arr(6).toLong & 0xFF) << 48) |
    ((arr(7).toLong & 0xFF) << 56)
  })

  // ULong
  implicit val ulongToByte: TypeConverter[ULong, Byte] = byteToULong.reverse
  implicit val ulongToUByte: TypeConverter[ULong, UByte] = ubyteToULong.reverse
  implicit val ulongToShort: TypeConverter[ULong, Short] = shortToULong.reverse
  implicit val ulongToUShort: TypeConverter[ULong, UShort] = ushortToULong.reverse
  implicit val ulongToInt: TypeConverter[ULong, Int] = intToULong.reverse
  implicit val ulongToUInt: TypeConverter[ULong, UInt] = uintToULong.reverse
  implicit val ulongToLong: TypeConverter[ULong, Long] = longToULong.reverse
  implicit val ulongToULong: TypeConverter[ULong, ULong] = identity
  implicit val ulongToFloat: TypeConverter[ULong, Float] = instance(ul => ul.toFloat, f => ULong.trunc(f.toLong))
  implicit val ulongToDouble: TypeConverter[ULong, Double] = instance(ul => ul.toDouble, d => ULong.trunc(d.toLong))
  implicit val ulongToString: TypeConverter[ULong, String] = instance(ul => ul.toString, s => ULong.trunc(s.toLong))
  implicit val ulongToBoolean: TypeConverter[ULong, Boolean] = instance(ul => ul.toLong != 0, bool => if (bool) ULong.One else ULong.Zero)
  implicit val ulongToBigInteger: TypeConverter[ULong, java.math.BigInteger] =
    instance(ul => java.math.BigInteger.valueOf(ul.toLong), bi => ULong.trunc(bi.longValueExact()))
  implicit val ulongToBigDecimal: TypeConverter[ULong, java.math.BigDecimal] =
    instance(ul => java.math.BigDecimal.valueOf(ul.toLong), bd => ULong.trunc(bd.longValueExact()))
  implicit val ulongToBytes: TypeConverter[ULong, Array[Byte]] = instance(
    ul => longToBytes.to(ul.toLong),
    arr => ULong.trunc(longToBytes.from(arr))
  )

  // Float
  implicit val floatToByte: TypeConverter[Float, Byte] = byteToFloat.reverse
  implicit val floatToUByte: TypeConverter[Float, UByte] = ubyteToFloat.reverse
  implicit val floatToShort: TypeConverter[Float, Short] = shortToFloat.reverse
  implicit val floatToUShort: TypeConverter[Float, UShort] = ushortToFloat.reverse
  implicit val floatToInt: TypeConverter[Float, Int] = intToFloat.reverse
  implicit val floatToUInt: TypeConverter[Float, UInt] = uintToFloat.reverse
  implicit val floatToLong: TypeConverter[Float, Long] = longToFloat.reverse
  implicit val floatToULong: TypeConverter[Float, ULong] = ulongToFloat.reverse
  implicit val floatToFloat: TypeConverter[Float, Float] = identity
  implicit val floatToDouble: TypeConverter[Float, Double] = instance(f => f.toDouble, d => d.toFloat)
  implicit val floatToString: TypeConverter[Float, String] = instance(f => f.toString, s => s.toFloat)
  implicit val floatToBoolean: TypeConverter[Float, Boolean] = instance(f => f != 0.0f, bool => if (bool) 1.0f else 0.0f)
  implicit val floatToBigInteger: TypeConverter[Float, java.math.BigInteger] =
    instance(f => java.math.BigDecimal.valueOf(f.toDouble).toBigIntegerExact, bi => bi.floatValue())
  implicit val floatToBigDecimal: TypeConverter[Float, java.math.BigDecimal] =
    instance(f => java.math.BigDecimal.valueOf(f.toDouble), bd => bd.floatValue())
  implicit val floatToBytes: TypeConverter[Float, Array[Byte]] = instance(f => intToBytes.to(java.lang.Float.floatToIntBits(f)), arr => java.lang.Float.intBitsToFloat(intToBytes.from(arr)))

  // Double
  implicit val doubleToByte: TypeConverter[Double, Byte] = byteToDouble.reverse
  implicit val doubleToUByte: TypeConverter[Double, UByte] = ubyteToDouble.reverse
  implicit val doubleToShort: TypeConverter[Double, Short] = shortToDouble.reverse
  implicit val doubleToUShort: TypeConverter[Double, UShort] = ushortToDouble.reverse
  implicit val doubleToInt: TypeConverter[Double, Int] = intToDouble.reverse
  implicit val doubleToUInt: TypeConverter[Double, UInt] = uintToDouble.reverse
  implicit val doubleToLong: TypeConverter[Double, Long] = longToDouble.reverse
  implicit val doubleToULong: TypeConverter[Double, ULong] = ulongToDouble.reverse
  implicit val doubleToFloat: TypeConverter[Double, Float] = floatToDouble.reverse
  implicit val doubleToDouble: TypeConverter[Double, Double] = identity
  implicit val doubleToString: TypeConverter[Double, String] = instance(d => d.toString, s => s.toDouble)
  implicit val doubleToBoolean: TypeConverter[Double, Boolean] = instance(d => d != 0.0, bool => if (bool) 1.0 else 0.0)
  implicit val doubleToBigInteger: TypeConverter[Double, java.math.BigInteger] =
    instance(d => java.math.BigDecimal.valueOf(d).toBigIntegerExact, bi => bi.doubleValue())
  implicit val doubleToBigDecimal: TypeConverter[Double, java.math.BigDecimal] =
    instance(d => java.math.BigDecimal.valueOf(d), bd => bd.doubleValue())
  implicit val doubleToBytes: TypeConverter[Double, Array[Byte]] = instance(d => longToBytes.to(java.lang.Double.doubleToLongBits(d)), arr => java.lang.Double.longBitsToDouble(longToBytes.from(arr)))

  // Boolean
  implicit val booleanToByte: TypeConverter[Boolean, Byte] = byteToBoolean.reverse
  implicit val booleanToUByte: TypeConverter[Boolean, UByte] = ubyteToBoolean.reverse
  implicit val booleanToShort: TypeConverter[Boolean, Short] = shortToBoolean.reverse
  implicit val booleanToUShort: TypeConverter[Boolean, UShort] = ushortToBoolean.reverse
  implicit val booleanToInt: TypeConverter[Boolean, Int] = intToBoolean.reverse
  implicit val booleanToUInt: TypeConverter[Boolean, UInt] = uintToBoolean.reverse
  implicit val booleanToLong: TypeConverter[Boolean, Long] = longToBoolean.reverse
  implicit val booleanToULong: TypeConverter[Boolean, ULong] = ulongToBoolean.reverse
  implicit val booleanToFloat: TypeConverter[Boolean, Float] = floatToBoolean.reverse
  implicit val booleanToDouble: TypeConverter[Boolean, Double] = doubleToBoolean.reverse
  implicit val booleanToString: TypeConverter[Boolean, String] = instance(bool => bool.toString, s => s.head match {
    case '1' | 't' | 'T' | 'y' | 'Y' => true
    case '0' | 'f' | 'F' | 'n' | 'N' => false
    case other                      => throw new IllegalArgumentException(s"Cannot convert String '$other' to Boolean")
  })
  implicit val booleanToBoolean: TypeConverter[Boolean, Boolean] = identity
  implicit val booleanToBigInteger: TypeConverter[Boolean, java.math.BigInteger] =
    instance(bool => if (bool) java.math.BigInteger.ONE else java.math.BigInteger.ZERO, bi => bi != java.math.BigInteger.ZERO)
  implicit val booleanToBigDecimal: TypeConverter[Boolean, java.math.BigDecimal] =
    instance(bool => if (bool) java.math.BigDecimal.ONE else java.math.BigDecimal.ZERO, bd => bd != java.math.BigDecimal.ZERO)
  implicit val booleanToBytes: TypeConverter[Boolean, Array[Byte]] = instance(bool => Array(if (bool) 1.toByte else 0.toByte), arr => arr.head != 0)

  // String
  implicit val stringToByte: TypeConverter[String, Byte] = byteToString.reverse
  implicit val stringToUByte: TypeConverter[String, UByte] = ubyteToString.reverse
  implicit val stringToShort: TypeConverter[String, Short] = shortToString.reverse
  implicit val stringToUShort: TypeConverter[String, UShort] = ushortToString.reverse
  implicit val stringToInt: TypeConverter[String, Int] = intToString.reverse
  implicit val stringToUInt: TypeConverter[String, UInt] = uintToString.reverse
  implicit val stringToLong: TypeConverter[String, Long] = longToString.reverse
  implicit val stringToULong: TypeConverter[String, ULong] = ulongToString.reverse
  implicit val stringToFloat: TypeConverter[String, Float] = floatToString.reverse
  implicit val stringToDouble: TypeConverter[String, Double] = doubleToString.reverse
  implicit val stringToString: TypeConverter[String, String] = identity
  implicit val stringToBoolean: TypeConverter[String, Boolean] = booleanToString.reverse
  implicit val stringToBigInteger: TypeConverter[String, java.math.BigInteger] =
    instance(s => new java.math.BigInteger(s), bi => bi.toString)
  implicit val stringToBigDecimal: TypeConverter[String, java.math.BigDecimal] =
    instance(s => new java.math.BigDecimal(s), bd => bd.toString)
  implicit val stringToBytes: TypeConverter[String, Array[Byte]] = instance(s => s.getBytes("UTF-8"), arr => new String(arr, "UTF-8"))

  def instance[S : ru.TypeTag, T : ru.TypeTag](toFun: S => T, fromFun: T => S): TypeConverter[S, T] =
    new TypeConverter[S, T] {
      override val to: S => T     = toFun
      override val from: T => S   = fromFun
    }

  def identity[T : ru.TypeTag]: TypeConverter[T, T] =
    instance[T, T](t => t, t => t)

  // ========= cache without ModuleMirror =========
  private val m  = ru.runtimeMirror(getClass.getClassLoader)
  private val me = this                                           // the singleton instance
  private val cls = me.getClass                                   // java.lang.Class of the module’s runtime class

  private val tcCtor = ru.typeOf[TypeConverter[_, _]].typeConstructor
  private val modType: ru.Type = m.classSymbol(cls).toType        // type of the module runtime class

  // Find getters whose *result type* is TypeConverter[_, _]
  private def entries: Iterable[((ru.Type, ru.Type), TypeConverter[_, _])] =
    modType.members.collect {
      case term: ru.TermSymbol
        if term.isPublic && term.isMethod && term.asMethod.isGetter =>
        val res = term.typeSignatureIn(modType).finalResultType.dealias.widen
        if (res.typeConstructor =:= tcCtor) {
          val List(srcT, dstT) = res.typeArgs.map(_.dealias)
          val name = term.name.toString.trim

          // invoke the getter via Java reflection
          val method = cls.getMethod(name)
          val value  = method.invoke(me).asInstanceOf[TypeConverter[_, _]]

          Some(((srcT, dstT), value))
        } else None
    }.flatMap(_.toList)

  private val cache = scala.collection.mutable.Map(entries.toSeq: _*)

  def register(converter: TypeConverter[_, _]): Unit = {
    val key = (converter.sourceType, converter.targetType)

    cache.put(key, converter)

    val reversed = converter.reverse
    val revKey = (reversed.sourceType, reversed.targetType)

    cache.put(revKey, reversed)
  }

  def get[S : ru.TypeTag, T : ru.TypeTag]: TypeConverter[S, T] = get(
    ru.typeOf[S].dealias,
    ru.typeOf[T].dealias
  ).asInstanceOf[TypeConverter[S, T]]

  def get(source: ru.Type, target: ru.Type): TypeConverter[_, _] = {
    (ReflectUtils.isOption(source), ReflectUtils.isOption(target)) match {
      case (true, true) =>
        val srcArg = ReflectUtils.typeArgument(source, 0)
        val tgtArg = ReflectUtils.typeArgument(target, 0)
        val baseConverter = get(srcArg, tgtArg)
        baseConverter.optional

      case (true, false) =>
        val srcArg = ReflectUtils.typeArgument(source, 0)
        val baseConverter = get(srcArg, target)
        baseConverter.optionalSource

      case (false, true) =>
        val tgtArg = ReflectUtils.typeArgument(target, 0)
        val baseConverter = get(source, tgtArg)
        baseConverter.optionalTarget

      case (false, false) =>
        cache.getOrElse((source, target), {
          throw new NoSuchElementException(s"No TypeConverter found for $source -> $target")
        })
    }
  }
}