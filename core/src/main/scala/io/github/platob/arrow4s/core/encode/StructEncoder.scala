package io.github.platob.arrow4s.core.encode

import org.apache.arrow.vector.complex.StructVector

case class StructEncoder[T](
  encoders: Seq[Encoder],
) extends Encoder.Typed[T, StructVector] {
  override def setValue(vector: StructVector, index: Int, value: T): Unit = {
    // Implementation to set the struct value in the StructVector
    // This is a placeholder implementation and should be replaced with actual logic
    vector.setIndexDefined(index)
    val productElements = value.asInstanceOf[Product].productIterator.toSeq
    val childVectors = vector.getChildrenFromFields

    encoders.zipWithIndex.foreach { case (encoder, childIndex) =>
      // Assuming value is a Product (like a case class or tuple)
      val fieldValue = productElements(childIndex)
      val childVector = childVectors.get(childIndex)

      encoder.unsafeSet(vector = childVector, index = index, value = fieldValue)
    }
  }
}
