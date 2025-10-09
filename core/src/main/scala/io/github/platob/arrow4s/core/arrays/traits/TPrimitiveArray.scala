package io.github.platob.arrow4s.core.arrays.traits

import io.github.platob.arrow4s.core.codec.ValueCodec

trait TPrimitiveArray extends TArrowArray {
  override def isPrimitive: Boolean = true

  override def isNested: Boolean = false

  override def isOptional: Boolean = false

  override def isLogical: Boolean = false

  @inline def setPrimitive[VC](index: Int, value: VC)(implicit codec: ValueCodec[VC]): this.type
}
