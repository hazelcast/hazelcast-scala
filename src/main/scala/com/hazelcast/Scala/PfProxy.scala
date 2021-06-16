package com.hazelcast.Scala

import scala.concurrent.ExecutionContext

private[Scala] class PfProxy[-E](pf: PartialFunction[E, Unit], ec: Option[ExecutionContext]) {
  private[this] val exec = ec.orNull
  protected final def invokeWith(evt: E): Unit =
    if (pf.isDefinedAt(evt)) {
      if (exec == null) pf(evt)
      else exec execute new Runnable {
        def run = pf(evt)
      }
    }
}
