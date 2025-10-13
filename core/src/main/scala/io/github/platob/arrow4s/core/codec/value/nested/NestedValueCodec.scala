package io.github.platob.arrow4s.core.codec.value.nested

import io.github.platob.arrow4s.core.codec.value.ValueCodec

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}

abstract class NestedValueCodec[T](
  val tpe: ru.Type,
  val typeTag: ru.TypeTag[T],
  val clsTag: ClassTag[T]
) extends ValueCodec[T] {
  override def namespace: String = s"${clsTag.runtimeClass.getSimpleName}[${tpe.typeArgs.map(_.typeSymbol.name).mkString(",")}]"

  override def isPrimitive: Boolean = false

  lazy val bitSize: Int = {
    // if all fixed size, sum them up
    val sizes = children.map(_.bitSize)

    if (!isOption && sizes.forall(_ > 0)) sizes.sum
    else -1
  }

  override def zero: T = {
    val arr = children.map(_.zero).toArray

    fromElements(arr)
  }

  override def one: T = {
    val arr = children.map(_.one).toArray

    fromElements(arr)
  }

  override def default: T = zero
}