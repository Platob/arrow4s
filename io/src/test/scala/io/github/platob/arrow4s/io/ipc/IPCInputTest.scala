package io.github.platob.arrow4s.io.ipc

import munit.FunSuite

class IPCInputTest extends FunSuite with IOFSSuite {
  test("load ipc file") {
    val path = loadResource("/ipc/test_file.arrow").toString
    val ipc = IPCInput.file(filePath = path)

    val batches = ipc.batches[(Int, Double)].toList
    val batch = batches.head
    val record = batch(0)

    assertEquals(batch.length, 5)
    assertEquals(record._1, 1)
  }
}
