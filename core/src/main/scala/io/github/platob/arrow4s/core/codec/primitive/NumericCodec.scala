package io.github.platob.arrow4s.core.codec.primitive

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class NumericCodec[T : ru.TypeTag : ClassTag] extends PrimitiveCodec[T] {
  val zero: T = fromInt(0)
  val one: T = fromInt(1)

  override def toBoolean(value: T): Boolean = if (value == zero) false else true
  override def fromBoolean(value: Boolean): T = if (value) one else zero
}
