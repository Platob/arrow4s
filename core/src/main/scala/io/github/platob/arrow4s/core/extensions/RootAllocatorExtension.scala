package io.github.platob.arrow4s.core.extensions

import org.apache.arrow.memory.RootAllocator

object RootAllocatorExtension {
  /** A singleton root allocator for general use.
    *
    * Note: You may want to create your own allocator for more complex applications
    * to better manage memory usage and avoid fragmentation.
    */
  val INSTANCE = new RootAllocator()
}
