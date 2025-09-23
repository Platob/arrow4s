package io.github.platob.arrow4s.core

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

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

  override def close(): Unit = {
    jrb.close()
    root.close()
  }
}
