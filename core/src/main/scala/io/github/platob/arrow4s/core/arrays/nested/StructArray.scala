package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.arrow.vector.{FieldVector, ValueVector, VectorSchemaRoot}

import scala.collection.convert.ImplicitConversions.{`iterable AsScalaIterable`, `map AsScala`}
import scala.reflect.runtime.universe

trait StructArray extends NestedArray.Typed[StructVector, ArrowRecord] {
  override def scalaType: universe.Type = universe.typeOf[ArrowRecord]

  def toJavaBatch: VectorSchemaRoot

  override def get(index: Int): ArrowRecord = {
    ArrowRecord.view(this, index)
  }

  override def set(index: Int, value: ArrowRecord): this.type = {
    for (i <- value.getIndices) {
      val child = childArray(i)
      val fieldValue = value.getAnyOrNull(i)

      child.setAnyOrNull(index, fieldValue)
    }

    this
  }
}

object StructArray {
  class JavaRoot(
    val arrowField: Field,
    val root: VectorSchemaRoot
  ) extends StructArray {
    override def vector: StructVector =
      throw new IllegalArgumentException(
        s"$this is only a view over VectorSchemaRoot, use toJavaBatch to get the root"
      )

    override val field: Field = arrowField

    override def childVector(index: Int): FieldVector = root.getVector(index)

    override def toJavaBatch: VectorSchemaRoot = root

    override def length: Int = root.getRowCount

    override def isNull(index: Int): Boolean = false

    override def setNull(index: Int): JavaRoot.this.type = {
      for (i <- 0 until this.cardinality) {
        val childVector = root.getVector(i)

        childVector.setNull(index)
      }

      this
    }
  }

  def from(root: VectorSchemaRoot): JavaRoot = {
    val arrowField = {
      val kids = root.getFieldVectors.map(_.getField).toList
      val meta = Option(root.getSchema.getCustomMetadata).map(_.toMap)

      ArrowField.build(
        "record",
        new ArrowType.Struct(),
        nullable = false,
        metadata = meta,
        children = kids
      )
    }

    new JavaRoot(arrowField, root)
  }

  def from(structVector: StructVector): StructArray = {
    new StructArray {
      override val vector: StructVector = structVector

      override def toJavaBatch: VectorSchemaRoot = {
        val children = vector.getChildrenFromFields.toSeq

        VectorSchemaRoot.of(children:_*)
      }

      override def childVector(index: Int): ValueVector = vector.getChildByOrdinal(index)

      override def isNull(index: Int): Boolean = vector.isNull(index)

      override def setNull(index: Int): this.type = {
        vector.setNull(index)

        this
      }
    }
  }
}