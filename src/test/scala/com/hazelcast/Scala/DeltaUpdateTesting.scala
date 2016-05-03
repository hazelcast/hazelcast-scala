package com.hazelcast.Scala

import java.util.UUID

import org.junit.Assert._

object DeltaUpdateTesting {

  type Map = KeyedDeltaUpdates[UUID, Int] { type UpdateR[T] = T }

  def testUpsert(numbers: Map, get: UUID => Option[Int], remove: UUID => Unit) {
    val key = UUID.randomUUID()
    numbers.upsert(key, 5)(_ + 1) match {
      case Update => fail("Should have been Insert")
      case Insert => assertEquals(Some(5), get(key))
    }
    numbers.upsert(key, 3)(_ + 9) match {
      case Update => assertEquals(Some(14), get(key))
      case Insert => fail("Should have been Update")
    }
    assertEquals(20, numbers.upsertAndGet(key, 7)(_ + 6))
    assertEquals(Some(20), numbers.getAndUpsert(key, 11)(_ + 3))
    assertEquals(Some(23), get(key))
    remove(key)
    assertEquals(None, numbers.getAndUpsert(key, 45)(_ + 2))
    assertEquals(Some(45), get(key))
  }

  def testUpdate(numbers: Map, get: UUID => Option[Int], insert: (UUID, Int) => Unit, remove: UUID => Unit) {
    val key = UUID.randomUUID()
    assertFalse(numbers.update(key)(_ + 1))
    insert(key, 3)
    assertTrue(numbers.update(key)(_ + 4))
    assertEquals(Some(7), get(key))
    assertEquals(Some(10), numbers.updateAndGet(key)(_ + 3))
    assertEquals(Some(10), numbers.getAndUpdate(key)(_ + 17))
    assertEquals(Some(27), get(key))
    remove(key)
    assertEquals(None, numbers.getAndUpdate(key)(_ + 2))
    assertEquals(None, get(key))
  }
}
