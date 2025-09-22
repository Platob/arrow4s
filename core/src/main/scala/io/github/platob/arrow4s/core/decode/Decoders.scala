package io.github.platob.arrow4s.core.decode

import io.github.platob.arrow4s.core.ArrowRecord
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.{IntVector, types}

import scala.language.implicitConversions

object Decoders {
  def from(field: types.pojo.Field): Decoder = {
    field.getType match {
      case types.Types.MinorType.INT =>
        intDecoder
      case types.Types.MinorType.STRUCT =>
        structDecoder(field.getChildren)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }

  implicit val intDecoder: Decoder.Typed[Int, IntVector] = new Decoder.Typed[Int, IntVector] {
    override def get(vector: IntVector, index: Int): Int = vector.get(index)
  }

  implicit def structDecoder(fields: java.util.List[Field]): Decoder.Typed[ArrowRecord, StructVector] = {
    new Decoder.Typed[ArrowRecord, StructVector] {
      override def get(vector: StructVector, index: Int): ArrowRecord = {
        val fieldVectors = vector.getChildrenFromFields
        val values = new Array[Any](fieldVectors.size())
        var i: Int = 0

        fieldVectors.stream().forEach(fieldVector => {
          val decoder = from(fieldVector.getField)
          val childValue = decoder.getAny(vector = fieldVector, index = index)
          i += 1

          values(i) = childValue
        })

        ArrowRecord(fields, values)
      }
    }
  }
}
