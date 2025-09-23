package io.github.platob.arrow4s.core

import org.apache.arrow.vector.types.pojo.Field

case class ArrowRecord(
  fields: java.util.List[Field],
  values: Array[Any]
) {
  def indices: Range = 0 until fields.size()

  def indexOf(name: String): Int = {
    for (i <- this.indices) {
      val field = fields.get(i)

      if (field.getName == name) {
        return i
      }
    }

    for (i <- this.indices) {
      val field = fields.get(i)

      if (field.getName.equalsIgnoreCase(name)) {
        return i
      }
    }

    -1
  }

  def field(index: Int): Field = {
    fields.get(index)
  }

  def get(index: Int): Any = {
    values(index)
  }

  def get(name: String): Any = {
    val index = indexOf(name)

    get(index)
  }

  def getOrNull(name: String): Any = {
    val index = indexOf(name)

    if (index == -1) {
      null
    } else {
      get(index)
    }
  }
}
