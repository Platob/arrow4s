package io.github.platob.arrow4s.core.codec.value.primitive

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class NumericValueCodec[T : ru.TypeTag : ClassTag] extends PrimitiveValueCodec[T] {
  val zero: T = fromInt(0)
  val one: T = fromInt(1)

  override def toBoolean(value: T): Boolean = if (value == zero) false else true
  override def fromBoolean(value: Boolean): T = if (value) one else zero
}
