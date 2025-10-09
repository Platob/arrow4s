package io.github.platob.arrow4s.io

import io.github.platob.arrow4s.core.arrays.nested.{ArrowRecord, StructArray}
import io.github.platob.arrow4s.core.codec.ValueCodec
import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.scanner.{ScanOptions, Scanner}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

trait DataInput {
  def format: FileFormat

  def scanOptions: ScanOptions

  def javaBatches(closeAtEnd: Boolean): Iterator[VectorSchemaRoot]

  def batches[T : ValueCodec]: Iterator[StructArray[T]] = {
    this.javaBatches(closeAtEnd = true).map(r => {
      StructArray.from[T](root = r)
    })
  }
}

object DataInput {
  trait File extends DataInput {
    val path: String

    def scanner: Scanner

    def javaBatches(closeAtEnd: Boolean): Iterator[VectorSchemaRoot] = {
      val r = this.scanner.scanBatches()

      new Iterator[VectorSchemaRoot] with AutoCloseable {
        private val reader = r
        private val root   = reader.getVectorSchemaRoot // Arrow reuses this per batch
        private var loaded = reader.loadNextBatch()     // prime the pump
        private var closed = false

        def close(): Unit =
          if (!closed) {
            try root.close() finally reader.close()
            closed = true
          }

        override def hasNext: Boolean = {
          if (!loaded && closeAtEnd) close()

          loaded
        }

        override def next(): VectorSchemaRoot = {
          if (!hasNext) throw new NoSuchElementException("no more batches")

          // advance to next batch
          loaded = reader.loadNextBatch()

          root
        }
      }
    }
  }
}