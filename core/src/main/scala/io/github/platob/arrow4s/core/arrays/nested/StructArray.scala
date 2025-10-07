package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.ArrowArray
import io.github.platob.arrow4s.core.entensions.FastCtorCache
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.util.TransferPair
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.reflect.runtime.{universe => ru}

class StructArray[ScalaType](
  val scalaType: ru.Type,
  val vector: StructVector,
  val childrenArrays: Seq[ArrowArray[_]],
  val getter: (StructArray[ScalaType], Int) => ScalaType,
  val setter: (StructArray[ScalaType], Int, ScalaType) => Unit
) extends NestedArray.Typed[StructVector, ScalaType] {
  private val arrayTL: Array[AnyRef] = new Array[AnyRef](cardinality) // reusable buffer for child values

  override def setValueCount(count: Int): StructArray.this.type = {
    vector.setValueCount(count)
    childrenArrays.foreach(_.setValueCount(count))

    this
  }

  override def innerAs(tpe: ru.Type): ArrowArray[_] = {
    val codec = FastCtorCache.codecFor(tpe)

    val casted =
      if (codec.tuple) {
        codec.fields.zipWithIndex.map {
          case ((_, fieldType), idx) =>
            val child = this.child(index = idx)
            // Cast child array to the expected type
            child.as(fieldType)
        }
      } else {
        codec.fields.map {
          case (name, fieldType) =>
            val child = this.child(name = name)
            // Cast child array to the expected type
            child.as(fieldType)
        }
      }

    new StructArray[AnyRef](
      scalaType = tpe,
      vector = vector,
      childrenArrays = casted,
      getter = (arr, index) => {
        var i = 0
        while (i < arr.arrayTL.length) {
          // child.get returns Any; MethodHandle wants AnyRef (boxed ok)
          arr.arrayTL(i) = arr.child(i).get(index).asInstanceOf[AnyRef]
          i += 1
        }
        codec.ctor.invokeWithArguments(arr.arrayTL: _*) // returns the case class instance
      },
      setter = (arr, index, value) => {
        arr.vector.setIndexDefined(index) // mark the struct itself non-null

        // explode the Product with zero iterator churn
        val p = value.asInstanceOf[Product]
        var i = 0
        while (i < arr.arrayTL.length) {
          val child = arr.child(i)
          val value = p.productElement(i) // Any
          child.unsafeSet(index, value)
          i += 1
        }
      }
    )
  }

  // Accessors
  override def get(index: Int): ScalaType = {
    getter(this, index)
  }

  // Mutators
  override def setNull(index: Int): this.type = {
    vector.setNull(index)

    this
  }

  override def set(index: Int, value: ScalaType): this.type = {
    setter(this, index, value)

    this
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

  def default(root: VectorSchemaRoot): StructArray[ArrowRecord] = {
    val structVector = toStructVector(root, fieldName = "root")

    default(structVector)
  }

  def default(structVector: StructVector): StructArray[ArrowRecord] = {
    new StructArray[ArrowRecord](
      scalaType = ru.typeOf[ArrowRecord],
      vector = structVector,
      childrenArrays = structVector.getChildrenFromFields.toSeq.map(ArrowArray.from),
      getter = (arr, index) => ArrowRecord.view(arr, index),
      setter = (arr, index, value) => {
        for (i <- value.indices) {
          val child = arr.childrenArrays(i)
          val fieldValue = value.getAny(i)

          child.unsafeSet(index, fieldValue)
        }

        this
      }
    )
  }
}