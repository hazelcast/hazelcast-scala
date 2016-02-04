package com.hazelcast.Scala

import com.hazelcast.ringbuffer.Ringbuffer

class HzRingbuffer[E](private val rb: Ringbuffer[E]) extends AnyVal {
  def async: AsyncRingbuffer[E] = new AsyncRingbuffer(rb)
}
