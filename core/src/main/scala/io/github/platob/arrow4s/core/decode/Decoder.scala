package io.github.platob.arrow4s.core.decode

import io.github.platob.arrow4s.core.ArrowRecord
import io.github.platob.arrow4s.core.decode.Decoders.intDecoder
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.{FieldVector, types}

import scala.jdk.CollectionConverters.CollectionHasAsScala

trait Decoder {
  def getAny(vector: FieldVector, index: Int): Any
}

object Decoder {
  abstract class Typed[T, V <: FieldVector] extends Decoder {
    def get(vector: V, index: Int): T

    def getOption(vector: V, index: Int): Option[T] = {
      if (vector.isNull(index)) None
      else Some(get(vector, index))
    }

    def getOrNull(vector: V, index: Int): T = {
      if (vector.isNull(index)) null.asInstanceOf[T]
      else get(vector, index)
    }

    def getAny(vector: FieldVector, index: Int): Any = {
      this.getOrNull(vector.asInstanceOf[V], index)
    }
  }

  def struct(fields: Seq[Field]): Decoder.Typed[ArrowRecord, StructVector] = {
    new Decoder.Typed[ArrowRecord, StructVector] {
      val decoders: Seq[Decoder] = fields.map(field => Decoder.from(field))

      override def get(vector: StructVector, index: Int): ArrowRecord = {
        val fieldVectors = vector.getChildrenFromFields
        val values = new Array[Any](fieldVectors.size())
        var i: Int = 0

        fieldVectors.stream().forEach(fieldVector => {
          val decoder = decoders(i)
          val childValue = decoder.getAny(vector = fieldVector, index = index)
          i += 1

          values(i) = childValue
        })

        ArrowRecord(fields, values)
      }
    }
  }

  def from(field: types.pojo.Field): Decoder = {
    field.getType.getTypeID match {
      case ArrowTypeID.Int =>
        intDecoder
      case ArrowTypeID.Struct =>
        struct(field.getChildren.asScala.toSeq)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }
}
