package io.github.platob.arrow4s.core.encode

import io.github.platob.arrow4s.core.ArrowRecord
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.{IntVector, types}

object Encoders {
  def from(field: types.pojo.Field): Encoder = {
    field.getType match {
      case types.Types.MinorType.INT =>
        intEncoder
      case types.Types.MinorType.STRUCT =>
        structEncoder
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }

  implicit val intEncoder: Encoder.Typed[Int, IntVector] = new Encoder.Typed[Int, IntVector] {
    override def setValue(vector: IntVector, value: Int, index: Int): Unit = {
      vector.set(index, value)
    }
  }

  implicit val structEncoder: Encoder.Typed[ArrowRecord, StructVector] = new Encoder.Typed[ArrowRecord, StructVector] {
    override def setValue(vector: StructVector, index: Int, value: ArrowRecord): Unit = {
      val fieldVectors = vector.getChildrenFromFields

      vector.setIndexDefined(index)

      fieldVectors.stream().forEach(fieldVector => {
        val encoder = from(fieldVector.getField)
        val childValue = value.getOrNull(name = fieldVector.getName)

        encoder.setAny(vector = fieldVector, index = index, value = childValue)
      })
    }
  }
}
