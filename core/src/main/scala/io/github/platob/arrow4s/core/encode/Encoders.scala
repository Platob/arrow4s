package io.github.platob.arrow4s.core.encode

import org.apache.arrow.vector.IntVector

object Encoders {
  implicit val intEncoder: Encoder.Typed[Int, IntVector] = new Encoder.Typed[Int, IntVector] {
    override def setValue(vector: IntVector, value: Int, index: Int): Unit = {
      vector.set(index, value)
    }
  }
}
