package io.github.platob.arrow4s.core

import io.github.platob.arrow4s.core.arrays.ArrowArray
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

import scala.jdk.CollectionConverters.CollectionHasAsScala

class RecordBatch(
  val js: ArrowSchema,
  val jrb: ArrowRecordBatch,
  val allocator: BufferAllocator
) extends AutoCloseable {
  private lazy val root: VectorSchemaRoot = {
    val r = VectorSchemaRoot.create(js, allocator)
    val loader: VectorLoader = new VectorLoader(root)

    loader.load(jrb)

    r
  }

  def arrays: Iterable[ArrowArray] = {
    root.getFieldVectors.asScala.map(ArrowArray.from)
  }

  override def close(): Unit = {
    jrb.close()
    root.close()
  }
}
