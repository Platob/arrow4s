package io.github.platob.arrow4s.io.ipc

import io.github.platob.arrow4s.core.arrays.nested.ArrowRecord
import munit.FunSuite

class IPCInputTest extends FunSuite with IOFSSuite {
  test("load ipc file") {
    val path = loadResource("/ipc/test_file.arrow").toString
    val ipc = IPCInput.file(filePath = path)

    val batches = ipc.batches(closeAtEnd = false).toList
    val batch = batches.head
    val record = batch(0)

    assertEquals(batch.length, 5)
    assert(record.isInstanceOf[ArrowRecord])
    assertEquals(record.getAnyOrNull(0), 1)
  }
}
