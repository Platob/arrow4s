package io.github.platob.arrow4s.io

import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.{ScanOptions, Scanner}
import org.apache.arrow.dataset.source.Dataset
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot

trait DataInput {
  def schemaRoots: Iterator[VectorSchemaRoot]
}

object DataInput {
  trait File extends DataInput {
    def factory: FileSystemDatasetFactory

    def scanOptions: ScanOptions

    def dataset: Dataset = {
      val ds = factory.finish()

      ds
    }

    def scanner: Scanner = {
      dataset.newScan(scanOptions)
    }

    def schemaRoots: Iterator[VectorSchemaRoot] = {
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
          if (!loaded) close()
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

  def file(
    path: String,
    format: FileFormat,
    scan: ScanOptions = new ScanOptions(64 * 1024),
    allocator: BufferAllocator = new RootAllocator(),
    cppMemoryPool: NativeMemoryPool = NativeMemoryPool.getDefault
  ): File = {
    val f = new FileSystemDatasetFactory(
      allocator,
      cppMemoryPool,
      format,
      path
    )

    new File {
      override val factory: FileSystemDatasetFactory = f

      override val scanOptions: ScanOptions = scan
    }
  }
}