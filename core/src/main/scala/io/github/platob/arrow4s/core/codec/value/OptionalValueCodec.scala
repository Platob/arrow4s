package io.github.platob.arrow4s.core.codec.value

import io.github.platob.arrow4s.core.types.ArrowField

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

class OptionalValueCodec[T](
  inner: ValueCodec[T],
  tpe: ru.Type,
  typeTag: ru.TypeTag[Option[T]],
  clsTag: ClassTag[Option[T]],
) extends ProxyCodec[T, Option[T]](
  arrowField = ArrowField.javaBuild(
    name = inner.arrowField.getName,
    at = inner.arrowField.getType,
    nullable = true,
    metadata = inner.arrowField.getMetadata,
    children = inner.arrowField.getChildren
  ),
  inner = inner, tpe = tpe, typeTag = typeTag, clsTag = clsTag
) {
  override val bitSize = -1 // Variable size

  def decode(value: Option[T]): T = value.getOrElse(inner.default)

  def encode(value: T): Option[T] = if (value == inner.default) None else Some(value)

  override def elementAt[E](value: Option[T], index: Int): E = value match {
    case Some(v) => v.asInstanceOf[E]
    case None => inner.default.asInstanceOf[E]
  }

  override def elements(value: Option[T]): Array[Any] = value match {
    case Some(v) => inner.elements(v)
    case None => Array.empty
  }

  override def fromElements(values: Array[Any]): Option[T] = {
    if (values.isEmpty) None
    else Some(inner.fromElements(values))
  }
}

object OptionalValueCodec {
  def make[T](implicit vc: ValueCodec[T]): OptionalValueCodec[T] = {
    implicit val baseTT: ru.TypeTag[T] = vc.typeTag
    implicit val baseCT: ClassTag[T] = vc.clsTag

    val tt = ru.typeTag[Option[T]]
    val tpe = tt.tpe
    val ct = classTag[Option[T]]

    new OptionalValueCodec[T](
      tpe = tpe,
      typeTag = tt,
      clsTag = ct,
      inner = vc
    )
  }
}
