package com.hazelcast.Scala

private[Scala] object TrueFunction extends (Any => Boolean) {
  def apply(any: Any) = true
  def apply[T]: T => Boolean = this.asInstanceOf[T => Boolean]
}
