package io.github.platob.arrow4s.core.codec.value

import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import scala.reflect.runtime.{universe => ru}

case class FieldCodec[T](
  field: Field,
  codec: ValueCodec[T]
) {
  def tpe: ru.Type = codec.tpe
  def fieldName: String = field.getName
  def arrowType: ArrowType = field.getType
}
