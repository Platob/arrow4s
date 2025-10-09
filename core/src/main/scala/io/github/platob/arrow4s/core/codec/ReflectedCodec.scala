package io.github.platob.arrow4s.core.codec

import scala.reflect.runtime.{universe => ru}
import scala.reflect.{ClassTag, classTag}

abstract class ReflectedCodec[T : ru.TypeTag : ClassTag] extends ValueCodec[T] {
  override val typeTag: ru.TypeTag[T] = implicitly[ru.TypeTag[T]]
  override val clsTag: ClassTag[T] = classTag[T]
  override val tpe: ru.Type = typeTag.tpe
  override def namespace: String = tpe.typeSymbol.fullName
}
