package com.hazelcast.Scala.serialization

import java.io.ByteArrayOutputStream

final class ByteArrayClassLoader(val className: String, val bytes: Array[Byte], fallback: Option[ClassLoader]) extends ClassLoader {
  def this(className: String, bytes: Array[Byte], fallback: ClassLoader) =
    this(className, bytes, Option(fallback))
  def this(className: String, bytes: Array[Byte]) =
    this(className, bytes, None)
  private lazy val cls = defineClass(className, bytes, 0, bytes.length)
  def loadClass(): Class[_] = loadClass(className)
  protected override def findClass(name: String): Class[_] = {
    if (name == className) cls
    else (fallback getOrElse getParent).loadClass(name)
    //  private def makeAccessible(cls: Class[_], seen: Set[Class[_]] = Set.empty): Set[Class[_]] = {
    //    if (cls != null && !seen(cls)) {
    //      cls.getDeclaredFields.foreach(_.setAccessible(true))
    //      cls.getDeclaredMethods.foreach(_.setAccessible(true))
    //      cls.getDeclaredConstructors.foreach(_.setAccessible(true))
    //      val seenMore = seen + cls
    //      //        cls.getDeclaredClasses.foldLeft(seen + cls) {
    //      //        case (seen, cls) => makeAccessible(cls, seen)
    //      //      }
    //      makeAccessible(cls.getSuperclass, seenMore)
    //    } else seen
  }
}
object ByteArrayClassLoader {
  def unapply(cl: ByteArrayClassLoader): (String, Array[Byte]) = cl.className -> cl.bytes
  def apply(cls: Class[_]): ByteArrayClassLoader = apply(cls.getName)
  private def apply(name: String): ByteArrayClassLoader = {
    val resourceName = "/" + name.replace('.', '/') + ".class"
    val is = getClass.getResourceAsStream(resourceName)
    try {
      val os = new ByteArrayOutputStream(4096)
      var b = is.read()
      while (b != -1) {
        os.write(b)
        b = is.read()
      }
      new ByteArrayClassLoader(name, os.toByteArray)
    } finally {
      is.close()
    }
  }
}
