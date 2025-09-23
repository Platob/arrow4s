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

  /**
   * Set the value at the given index, handling nulls appropriately.
   * @param vector Input vector
   * @param index Index to set
   * @param value Value to set, can be null
   */
  @inline def setAnyValue(vector: FieldVector, index: Int, value: Any): Unit

  /**
   * Set multiple values starting at the given index.
   * @param vector Input vector
   * @param startIndex Starting index to set
   * @param values Values to set
   */
  @inline def setAnyValues(vector: FieldVector, startIndex: Int, values: Iterable[Any]): Unit = {
    this.ensureCapacity(vector, startIndex + values.size)

    values.zipWithIndex.foreach { case (value, i) =>
      setAnyValue(
        vector = vector,
        index = startIndex + i,
        value = value
      )
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

    /**
     * Set the value at the given index, handling nulls appropriately.
     * @param vector Input vector
     * @param index Index to set
     * @param value Value to set, can be null
     */
    @inline def setAnyValue(vector: FieldVector, index: Int, value: Any): Unit = {
      if (value == null) {
        setNull(
          vector = vector.asInstanceOf[V],
          index = index
        )
      } else {
        set(
          vector = vector.asInstanceOf[V],
          index = index,
          value = value.asInstanceOf[T]
        )
      }
    }
  }

  def proxy[Logical, Primitive, V <: FieldVector](
    encoder: Typed[Primitive, V],
    apply: Logical => Primitive
  ): Typed[Logical, V] = new Typed[Logical, V] {
    def set(vector: V, index: Int, value: Logical): Unit = {
      encoder.set(vector, index, apply(value))
    }
  }
}
