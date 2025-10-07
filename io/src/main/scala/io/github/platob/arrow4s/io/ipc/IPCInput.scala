package io.github.platob.arrow4s.io.ipc

import io.github.platob.arrow4s.core.extensions.RootAllocatorExtension
import io.github.platob.arrow4s.io.DataInput
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.{ScanOptions, Scanner}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

trait IPCInput extends DataInput {
  override def format: FileFormat = FileFormat.ARROW_IPC
}

object IPCInput {
  trait File extends IPCInput with DataInput.File {

  }

  def file(filePath: String): File = file(
    filePath,
    new ScanOptions(64 * 1024),
    RootAllocatorExtension.INSTANCE,
    NativeMemoryPool.getDefault
  )

  def file(
    filePath: String,
    scan: ScanOptions,
    allocator: BufferAllocator,
    memoryPool: NativeMemoryPool
  ): File = {
    new IPCInput.File {
      override val path: String = filePath

      override def scanOptions: ScanOptions = scan

      def datasetFactory: FileSystemDatasetFactory = {
        new FileSystemDatasetFactory(
          allocator,
          memoryPool,
          this.format,
          path,
        )
      }

      override lazy val scanner: Scanner = this.datasetFactory
        .finish()
        .newScan(scanOptions)
    }
  }
}
