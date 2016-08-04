package com.hazelcast.Scala.serialization

import java.lang.ref.SoftReference
import scala.collection.mutable.Buffer

private[serialization] class SoftThreadLocal[T <: AnyRef](ctor: => T) {
  private class Instances {
    private[this] def newRef: SoftReference[T] = new SoftReference(ctor)
    private[this] def newRef(t: T): SoftReference[T] = new SoftReference(t)
    private[this] val buffer = Buffer(newRef)
    private[this] var idx = 0
    def use[R](thunk: T => (T, R)): R = {
      if (idx < buffer.length) {
        buffer(idx).get match {
          case null =>
            buffer(idx) = newRef
            use(thunk)
          case t =>
            idx += 1
            val (rt, r) = try thunk(t) finally idx -= 1
            if (t ne rt) buffer(idx) = newRef(rt)
            r
        }
      } else {
        buffer += newRef
        use(thunk)
      }
    }
  }
  private[this] val tl = new ThreadLocal[Instances] {
    override def initialValue = new Instances
  }
  final def use[R](thunk: T => (T, R)): R = tl.get.use(thunk)
}
