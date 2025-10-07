package io.github.platob.arrow4s.core.memory

import org.apache.arrow.memory.RootAllocator

object RootAllocatorExtension {
  val INSTANCE = new RootAllocator()
}
