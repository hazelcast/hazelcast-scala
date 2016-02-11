package com.hazelcast.Scala.serialization

import java.io.ByteArrayOutputStream

private[serialization] final class ByteArrayClassLoader(val className: String, val bytes: Array[Byte], fallback: Option[ClassLoader]) extends ClassLoader {
  def this(className: String, bytes: Array[Byte], fallback: ClassLoader) =
    this(className, bytes, Option(fallback))
  def this(className: String, bytes: Array[Byte]) =
    this(className, bytes, None)
  private lazy val cls = defineClass(className, bytes, 0, bytes.length)
  def loadClass(): Class[_] = loadClass(className)
  protected override def findClass(name: String): Class[_] = {
    if (name == className) cls
    else (fallback getOrElse getParent).loadClass(name)
  }
}
object ByteArrayClassLoader {
  def unapply(cl: ByteArrayClassLoader): (String, Array[Byte]) = cl.className -> cl.bytes
  def apply(cls: Class[_]): ByteArrayClassLoader = {
    val name = cls.getName
    val resourceName = s"/${name.replace('.', '/')}.class"
    val is = cls.getResourceAsStream(resourceName)
    require(is != null, s"Cannot find class file: $resourceName")
    try {
      val arr = new Array[Byte](4096)
      val os = new ByteArrayOutputStream(arr.length)
      var len = is.read(arr)
      while (len != -1) {
        if (len != 0) os.write(arr, 0, len)
        len = is.read(arr)
      }
      new ByteArrayClassLoader(name, os.toByteArray)
    } finally {
      is.close()
    }
  }
}
