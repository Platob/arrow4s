package io.github.platob.arrow4s.core.arrays.nested

import org.apache.arrow.vector.types.pojo.Field

trait ArrowRecord {
  override def toString: String = {
    s"[${this.getValues.mkString(", ")}]"
  }

  def getIndices: Range

  def getFieldAt(index: Int): Field

  def getValues: Seq[_] = {
    this.getIndices.map(this.getAnyOrNull)
  }

  def indexOf(name: String): Int = {
    for (i <- this.getIndices) {
      val field = getFieldAt(i)

      if (field.getName == name) {
        return i
      }
    }

    for (i <- this.getIndices) {
      val field = getFieldAt(i)

      if (field.getName.equalsIgnoreCase(name)) {
        return i
      }
    }

    -1
  }

  @inline def getAnyOrNull(index: Int): Any
}

object ArrowRecord {
  @inline def view(array: StructArray, index: Int): ArrowRecord = {
    new ArrowRecord {
      override def getIndices: Range = 0 until array.cardinality

      override def getFieldAt(i: Int): Field = array.childArray(i).field

      override def getAnyOrNull(i: Int): Any = array.childArray(i).getAnyOrNull(index)
    }
  }
}