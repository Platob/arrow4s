package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.extensions.FastCtorCache
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.util.TransferPair
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.reflect.runtime.{universe => ru}

class StructArray(
  val vector: StructVector,
  val children: Seq[ArrowArray.Typed[_, _]],
) extends NestedArray.Typed[StructVector, ArrowRecord] {
  override val scalaType: ru.Type = ru.typeOf[ArrowRecord]

  override def setValueCount(count: Int): StructArray.this.type = {
    vector.setValueCount(count)
    children.foreach(_.setValueCount(count))

    this
  }

  // Accessors
  override def get(index: Int): ArrowRecord = {
    ArrowRecord.view(array = this, index = index)
  }

  // Mutators
  override def setNull(index: Int): this.type = {
    vector.setNull(index)

    this
  }

  override def set(index: Int, value: ArrowRecord): this.type = {
    vector.setIndexDefined(index) // mark the struct itself non-null

    var i = 0
    value.toArray.foreach(v => {
      val child = children(i)
      child.unsafeSet(index, v)
      i += 1
    })

    this
  }

  override def innerAs(tpe: ru.Type): LogicalArray[StructArray, StructVector, ArrowRecord, AnyRef] = {
    val codec = FastCtorCache.codecFor(tpe)

    val casted =
      if (codec.tuple) {
        codec.fields.zipWithIndex.map {
          case (field, idx) =>
            val child = this.children(idx)
            // Cast child array to the expected type
            child.as(field.tpe)
        }
      } else {
        codec.fields.map(field => {
          val child = this.child(name = field.name)

          // Cast child array to the expected type
          child.as(field.tpe)
        })
      }

    val getter = (arr: LogicalArray[StructArray, StructVector, ArrowRecord, _], index: Int) => {
      val values = arr.children.map(child => {
        val value = child.get(index)

        value
      })

      codec.ctor.invokeWithArguments(values: _*) // returns the case class instance
    }

    val setter = (arr: LogicalArray[StructArray, StructVector, ArrowRecord, _], index: Int, value: Any) => {
      arr.vector.setIndexDefined(index) // mark the struct itself non-null

      // explode the Product with zero iterator churn
      val values = value.asInstanceOf[Product].productIterator.toSeq
      var i = 0

      values.foreach(v => {
        val child = arr.children(i)
        child.unsafeSet(index, v)
        i += 1
      })
    }

    new LogicalArray[StructArray, StructVector, ArrowRecord, AnyRef](
      scalaType = tpe,
      inner = this,
      getter = getter,
      setter = setter,
      children = casted,
    )
  }

  def toRoot: VectorSchemaRoot = StructArray.toStructRoot(vector)
}

object StructArray {
  def toStructVector(root: VectorSchemaRoot, fieldName: String): StructVector = {
    val field = ArrowField.build(
      name = fieldName,
      at = ArrowType.Struct.INSTANCE,
      nullable = false,
      children = root.getSchema.getFields.toList,
      metadata = None,
    )

    toStructVector(root, field)
  }

  /** Zero-copy *move*: root -> StructVector (buffers transferred, not duplicated). */
  def toStructVector(root: VectorSchemaRoot, structField: Field): StructVector = {
    // pull allocator from a column in the root (since root#getAllocator may not exist)
    val rootFieldVectors = root.getFieldVectors
    require(!rootFieldVectors.isEmpty, "VectorSchemaRoot has no columns; cannot pack into a struct")

    val rootAlloc: BufferAllocator = rootFieldVectors.get(0).getAllocator

    // create the StructVector with the requested allocator
    val struct = structField.createVector(rootAlloc).asInstanceOf[StructVector]

    // initialize struct children to match the root schema you’re packing
    val childFields = root.getSchema.getFields
    struct.initializeChildrenFromFields(childFields)

    var i = 0
    val rowCount = root.getRowCount

    while (i < childFields.size) {
      val f    = childFields.get(i)
      val src  = root.getVector(i)
      val dest = struct.getChild(f.getName)
      val tp: TransferPair = src.makeTransferPair(dest)

      tp.transfer()

      // keep child counts in sync (transfer usually handles this, copy doesn’t)
      dest.setValueCount(rowCount)
      i += 1
    }

    // Set validity
    for (j <- 0 until rowCount) {
      struct.setIndexDefined(j)
    }

    struct.setValueCount(rowCount)
    struct
  }

  /** Zero-copy share: StructVector -> VectorSchemaRoot (children reused as-is). */
  def toStructRoot(struct: StructVector): VectorSchemaRoot = {
    val children: java.util.List[FieldVector] = struct.getChildrenFromFields
    val schema   = new Schema(struct.getField.getChildren)
    val root     = new VectorSchemaRoot(schema, children, struct.getValueCount)
    root
  }

  def default(root: VectorSchemaRoot): StructArray = {
    val structVector = toStructVector(root, fieldName = "root")

    default(structVector)
  }

  def default(structVector: StructVector): StructArray = {
    new StructArray(
      vector = structVector,
      children = structVector.getChildrenFromFields.toSeq.map(ArrowArray.from)
    )
  }
}