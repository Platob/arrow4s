package io.github.platob.arrow4s.core.encode

import org.apache.arrow.vector._

trait Encoder {
  /**
   * Ensure that the vector has at least the given capacity.
   * @param vector Input vector
   * @param capacity Required capacity
   */
  @inline def ensureCapacity(vector: FieldVector, capacity: Int): Unit = {
    if (capacity > vector.getValueCapacity) {
      vector.setValueCount(capacity)
    }
  }
}

object Encoder {
  trait Typed[T, V <: FieldVector] extends Encoder {
    /**
     * Set the value at the given index.
     * @param vector Input vector
     * @param index Index to set
     * @param value Value to set
     */
    @inline def set(vector: V, index: Int, value: T): Unit

    /**
     * Set the value at the given index, or set to null if the value is null.
     * @param vector Input vector
     * @param index Index to set
     * @param value Value to set, or null
     */
    @inline def setOrNull(vector: V, index: Int, value: T): Unit = {
      if (value == null.asInstanceOf[T]) vector.setNull(index)
      else set(vector, index, value)
    }

    /**
     * Set the value at the given index.
     * @param vector Input vector
     * @param index Index to set
     * @param value Optional value to set
     */
    @inline def setOption(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => set(vector, index, v)
        case None => vector.setNull(index)
      }
    }

    /**
     * Safely set the value at the given index, ensuring capacity first.
     * @param vector Input vector
     * @param index Index to set
     * @param value Value to set
     */
    @inline def setSafe(vector: V, index: Int, value: T): Unit = {
      this.ensureCapacity(vector, index + 1)

      set(vector = vector, index = index, value = value)
    }

    /**
     * Safely set multiple values starting at the given index, ensuring capacity first.
     * @param vector Input vector
     * @param startIndex Starting index to set
     * @param values Values to set
     */
    @inline def setSafe(vector: V, startIndex: Int, values: Iterable[T]): Unit = {
      this.ensureCapacity(vector, startIndex + values.size)

      values.zipWithIndex.foreach { case (value, i) =>
        set(vector = vector, index = startIndex + i, value = value)
      }
    }

    /**
     * Set the value at the given index to null.
     * @param vector Input vector
     * @param index Index to set to null
     */
    @inline def setNull(vector: V, index: Int): Unit = {
      vector.setNull(index)
    }
  }
}
