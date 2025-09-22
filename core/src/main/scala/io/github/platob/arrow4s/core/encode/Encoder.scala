package io.github.platob.arrow4s.core.encode

import org.apache.arrow.vector._

trait Encoder {
  def unsafeSet(vector: FieldVector, index: Int, value: Any): Unit
}

object Encoder {
  abstract class Typed[T, V <: FieldVector] extends Encoder {
    def setValue(vector: V, index: Int, value: T): Unit

    private def setOptional(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => setValue(vector, index, v)
        case None => vector.setNull(index)
      }
    }

    def unsafeSet(vector: FieldVector, index: Int, value: Any): Unit = {
      val safeValue = Option(value).map(_.asInstanceOf[T])

      setOptional(vector.asInstanceOf[V], index, safeValue)
    }
  }
}
