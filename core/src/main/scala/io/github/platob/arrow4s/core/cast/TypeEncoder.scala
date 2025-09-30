package io.github.platob.arrow4s.core.cast

import org.apache.arrow.vector.FieldVector

import scala.reflect.runtime.{universe => ru}

abstract sealed class TypeEncoder[V <: FieldVector, Target] {
  @inline def get(vector: V, index: Int): Target

  @inline def getOrNull(vector: V, index: Int): Option[Target] = {
    if (vector.isNull(index)) None
    else Some(get(vector, index))
  }

  @inline def set(vector: V, index: Int, value: Target): Unit

  @inline def setOrNull(vector: V, index: Int, value: Option[Target]): Unit = {
    value match {
      case Some(v) => set(vector, index, v)
      case None => vector.setNull(index)
    }
  }
}

object TypeEncoder {
  def instance[V <: FieldVector, Target : ru.TypeTag](
    getter: (V, Int) => Target,
    setter: (V, Int, Target) => Unit
  ): TypeEncoder[V, Target] = {
    new TypeEncoder[V, Target] {
      override def get(vector: V, index: Int): Target = getter(vector, index)

      override def set(vector: V, index: Int, value: Target): Unit = setter(vector, index, value)
    }
  }

  def proxy[V <: FieldVector, Source : ru.TypeTag](
    source: ru.Type,
    target: ru.Type,
    encoder: TypeEncoder[V, Source]
  ): TypeEncoder[V, _] = {
    if (source =:= target) {
      return encoder
    }

    val converter = TypeConverter.get(source, target)

    converter match {
      case c: TypeConverter[V, t] @unchecked =>
        val typedConverter = c.asInstanceOf[TypeConverter[Source, t]]

        new TypeEncoder[V, t] {
          override def get(vector: V, index: Int): t = {
            val intermediate = encoder.get(vector, index)

            typedConverter.to(intermediate)
          }

          override def set(vector: V, index: Int, value: t): Unit = {
            val intermediate = typedConverter.from(value)

            encoder.set(vector, index, intermediate)
          }
        }
    }
  }
}
