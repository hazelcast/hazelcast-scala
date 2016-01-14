package com.hazelcast.Scala

import com.hazelcast.ringbuffer.Ringbuffer

class HzRingBuffer[E](private val rb: Ringbuffer[E]) extends AnyVal {
  def async: AsyncRingBuffer[E] = new AsyncRingBuffer(rb)
}
