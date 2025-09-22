package io.github.platob.arrow4s.core

import org.apache.arrow.vector.types.pojo.Schema

case class ArrowRecord(
  schema: Schema,
  values: Seq[Any]
)

object ArrowRecord {

}
