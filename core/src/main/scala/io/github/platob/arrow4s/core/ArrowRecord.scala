package io.github.platob.arrow4s.core

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.vector.types.pojo.Field

trait ArrowRecord {
  def fields: Seq[Field]

  def indices: Range = fields.indices

  def indexOf(name: String): Int = {
    for (i <- this.indices) {
      val field = fields(i)

      if (field.getName == name) {
        return i
      }
    }

    for (i <- this.indices) {
      val field = fields(i)

      if (field.getName.equalsIgnoreCase(name)) {
        return i
      }
    }

    -1
  }
}

object ArrowRecord {
  class View(
    val index: Int,
    val arrays: Seq[ArrowArray]
  ) extends ArrowRecord {
    def fields: Seq[Field] = arrays.map(_.field)
  }
}