package io.github.platob.arrow4s.io.ipc

import java.net.URL

trait IOFSSuite {
  def loadResource(name: String): URL = {
    val url = getClass.getResource(name)

    if (url == null) {
      throw new IllegalArgumentException(s"Resource not found: $name")
    }

    url
  }
}
