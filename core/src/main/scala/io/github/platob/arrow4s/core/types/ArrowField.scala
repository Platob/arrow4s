package io.github.platob.arrow4s.core.types

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

object ArrowField {
  def javaBuild(
    name: String,
    at: ArrowType,
    nullable: Boolean,
    children: java.util.List[Field],
    metadata: java.util.Map[String, String]
  ): Field = {
    val fieldType = new FieldType(nullable, at, null, metadata)

    new Field(name, fieldType, children)
  }

  def build(
    name: String,
    at: ArrowType,
    nullable: Boolean,
    children: Seq[Field],
    metadata: Option[Map[String, String]]
  ): Field = {
    val jChildren = children.asJava
    val jMetadata = metadata.map(_.asJava).orNull

    javaBuild(name, at, nullable, jChildren, jMetadata)
  }

  def rename(field: Field, newName: String): Field = {
    if (field.getName == newName) {
      return field
    }

    javaBuild(
      name = newName,
      at = field.getType,
      nullable = field.isNullable,
      children = field.getChildren,
      metadata = field.getMetadata
    )
  }

  def struct(
    name: String,
    children: Seq[Field],
    nullable: Boolean,
    metadata: Option[Map[String, String]]
  ): Field = {
    build(name, ArrowType.Struct.INSTANCE, nullable, children, metadata)
  }
}
