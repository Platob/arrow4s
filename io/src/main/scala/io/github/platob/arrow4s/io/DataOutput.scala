package io.github.platob.arrow4s.io

import org.apache.arrow.vector.VectorSchemaRoot

trait DataOutput extends AutoCloseable {
  def writeRoot(root: VectorSchemaRoot): Unit
}

object DataOutput {

}
