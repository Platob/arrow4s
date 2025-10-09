package io.github.platob.arrow4s.core.arrays.nested

import io.github.platob.arrow4s.core.arrays.{ArrowArray, LogicalArray}
import io.github.platob.arrow4s.core.codec.ValueCodec
import io.github.platob.arrow4s.core.codec.nested.{StructCodec, Tuple2Codec}
import io.github.platob.arrow4s.core.types.ArrowField
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.util.TransferPair
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class StructArray[R](
  vector: StructVector,
  codec: ValueCodec[R],
  val children: Seq[ArrowArray.Typed[_, _, _]]
) extends NestedArray.Typed[R, StructVector, StructArray[R]](vector, codec) {

  override def setValueCount(count: Int): this.type = {
    super.setValueCount(count)
    children.foreach(_.setValueCount(count))

    this
  }

  // Accessors
  override def get(index: Int): R = {
//    ArrowRecord.view(array = this, index = index)
    val elements = Array.tabulate(children.length)(i => {
      val child = children(i)
      child.get(index)
    })

    this.codec.fromElements(elements)
  }

  // Mutators
  /**
   * Marks the struct at the given index as non-null.
   * @param index the index to mark as non-null
   * @return
   */
  @inline def setIndexDefined(index: Int): this.type = {
    vector.setIndexDefined(index)

    this
  }

  override def setNull(index: Int): this.type = {
    vector.setNull(index)

    this
  }

  override def set(index: Int, value: R): this.type = {
    setIndexDefined(index) // mark the struct itself non-null

    var i = 0
    val elements = this.codec.elements(value)

    elements.foreach(v => {
      val child = childAt(i)
      child.unsafeSet(index, v)
      i += 1
    })

    this
  }

  override def innerAs(codec: ValueCodec[_]): ArrowArray[_] = {
    val casted =
      if (codec.isTuple) {
        codec.children.zipWithIndex.map {
          case (field, idx) =>
            val child = this.children(idx)
            // Cast child array to the expected type
            child.asUnsafe(field)
        }
      } else {
        codec.arrowChildren.map(fieldCodec => {
          val child = this.child(name = fieldCodec.field.getName)

          // Cast child array to the expected type
          child.asUnsafe(fieldCodec.codec)
        })
      }

    codec match {
      case c: ValueCodec[s] @unchecked =>
        LogicalArray.struct[R, s](this, c, casted)
    }
  }

  def toRoot: VectorSchemaRoot = StructArray.toStructRoot(vector)
}

object StructArray {
  private def toStructVector(root: VectorSchemaRoot, fieldName: String): StructVector = {
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
  private def toStructVector(root: VectorSchemaRoot, structField: Field): StructVector = {
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
  private def toStructRoot(struct: StructVector): VectorSchemaRoot = {
    val children: java.util.List[FieldVector] = struct.getChildrenFromFields
    val schema   = new Schema(struct.getField.getChildren)
    val root     = new VectorSchemaRoot(schema, children, struct.getValueCount)
    root
  }

  def from[T : ValueCodec](root: VectorSchemaRoot): StructArray[T] = {
    val structVector = toStructVector(root, fieldName = "root")

    from[T](structVector)
  }

  def from[T](structVector: StructVector)(implicit codec: ValueCodec[T]): StructArray[T] = {
    new StructArray[T](
      vector = structVector,
      children = structVector.getChildrenFromFields.toList.map(ArrowArray.from),
      codec = codec
    )
  }

  def default(structVector: StructVector): StructArray[_] = {
    val children = structVector.getChildrenFromFields.toList.map(ArrowArray.from)
    val codec = StructCodec.fromChildren(children.map(_.codec), name = "struct", nullable = false)

    codec match {
      case c: ValueCodec[t] @unchecked =>
        new StructArray[t](
          vector = structVector,
          children = children,
          codec = c
        )
    }
  }

  def tuple2[T1, T2](
    vector: StructVector,
    t1: ArrowArray.Typed[T1, _, _],
    t2: ArrowArray.Typed[T2, _, _]
  ): StructArray[(T1, T2)] = {
    require(vector.getChildrenFromFields.size() > 1, "StructVector must have at least two children")

    val codec = Tuple2Codec.fromCodecs[T1, T2](t1.codec, t2.codec, arrowField = Some(vector.getField))

    new StructArray[(T1, T2)](vector = vector, children = Seq(t1, t2), codec = codec)
  }

  def tuple2(vector: StructVector): StructArray[(_, _)] = {
    require(vector.getChildrenFromFields.size() > 1, "StructVector must have at least two children")

    val t1 = ArrowArray.from(vector.getChildByOrdinal(0))
    val t2 = ArrowArray.from(vector.getChildByOrdinal(1))

    (t1, t2) match {
      case (a1: ArrowArray.Typed[t1, _, _], a2: ArrowArray.Typed[t2, _, _]) =>
        tuple2[t1, t2](vector, a1, a2).asInstanceOf[StructArray[(_, _)]]
    }
  }
}