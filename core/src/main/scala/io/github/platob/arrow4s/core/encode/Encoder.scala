package io.github.platob.arrow4s.core.encode

import io.github.platob.arrow4s.core.ArrowRecord
import io.github.platob.arrow4s.core.encode.Encoders.intEncoder
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.Field

import scala.jdk.CollectionConverters.CollectionHasAsScala

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

  def struct(fields: Seq[Field]): Encoder.Typed[ArrowRecord, StructVector] = {
    new Encoder.Typed[ArrowRecord, StructVector] {
      val encoders: Seq[Encoder] = fields.map(field => Encoder.from(field))

      override def set(vector: StructVector, index: Int, value: ArrowRecord): Unit = {
        val fieldVectors = vector.getChildrenFromFields
        var i: Int = 0

        vector.setIndexDefined(index)

        fieldVectors.stream().forEach(fieldVector => {
          val encoder = encoders(i)
          val childValue = value.getOrNull(name = fieldVector.getName)

          encoder.setAnyValue(vector = fieldVector, index = index, value = childValue)
          i += 1
        })
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

  def optional[T, V <: FieldVector](encoder: Typed[T, V]): Typed[Option[T], V] = new Typed[Option[T], V] {
    def set(vector: V, index: Int, value: Option[T]): Unit = {
      value match {
        case Some(v) => encoder.set(vector, index, v)
        case None => vector.setNull(index)
      }
    }
  }
  
  def from(field: types.pojo.Field): Encoder = {
    field.getType.getTypeID match {
      case ArrowTypeID.Int =>
        intEncoder
      case ArrowTypeID.Struct =>
        struct(field.getChildren.asScala.toSeq)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported data type: ${field.getType}")
    }
  }
}
