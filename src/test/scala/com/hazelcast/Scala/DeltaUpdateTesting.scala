package com.hazelcast.Scala

import java.util.UUID

import org.junit.Assert._

object DeltaUpdateTesting {

  type Map = DeltaUpdates[UUID, Int] { type UpdateR[T] = T }

  def testUpsert(numbers: Map, get: UUID => Int) {
    val key = UUID.randomUUID()
    numbers.upsert(key, 5)(_ + 1) match {
      case Update => fail("Should have been Insert")
      case Insert => assertEquals(5, get(key))
    }
    numbers.upsert(key, 3)(_ + 9) match {
      case Update => assertEquals(14, get(key))
      case Insert => fail("Should have been Update")
    }
    assertEquals(20, numbers.upsertAndGet(key, 7)(_ + 6))

  }

  def testUpdate(numbers: Map, get: UUID => Int, insert: (UUID, Int) => Unit) {
    val key = UUID.randomUUID()
    assertFalse(numbers.update(key)(_ + 1))
    insert(key, 3)
    assertTrue(numbers.update(key)(_ + 4))
    assertEquals(7, get(key))
    assertEquals(Some(10), numbers.updateAndGet(key)(_ + 3))
  }
}
