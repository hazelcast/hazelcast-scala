package com.hazelcast.Scala

import java.util.UUID

import org.junit.Assert._
import com.hazelcast.core.IExecutorService

object DeltaUpdateTesting {

  type Map = KeyedDeltaUpdates[UUID, Int] { type UpdateR[T] = T }

  def testUpsert(numbers: Map, get: UUID => Option[Int], remove: UUID => Unit, runOn: IExecutorService = null) {
    val key = UUID.randomUUID()
    numbers.upsert(key, 5, runOn)(_ + 1) match {
      case WasUpdated => fail("Should have been Insert")
      case WasInserted => assertEquals(Some(5), get(key))
    }
    numbers.upsert(key, 3, runOn)(_ + 9) match {
      case WasInserted => fail("Should have been Update")
      case WasUpdated => assertEquals(Some(14), get(key))
    }
    assertEquals(20, numbers.upsertAndGet(key, 7, runOn)(_ + 6))
    assertEquals(Some(20), numbers.getAndUpsert(key, 11, runOn)(_ + 3))
    assertEquals(Some(23), get(key))
    remove(key)
    assertEquals(None, numbers.getAndUpsert(key, 45, runOn)(_ + 2))
    assertEquals(Some(45), get(key))
    assertEquals(99, numbers.upsertAndGet(UUID.randomUUID, 99, runOn)(_ + 55))
  }

  def testUpdate(numbers: Map, get: UUID => Option[Int], insert: (UUID, Int) => Unit, remove: UUID => Unit, runOn: IExecutorService = null) {
    val key = UUID.randomUUID()
    assertEquals(None, numbers.updateAndGet(key, runOn)(_ + 3))
    assertEquals(None, numbers.updateAndGetIf(_ => true, key, runOn)(_ + 3))
    assertEquals(None, numbers.getAndUpdateIf(_ => true, key, runOn)(_ + 3))
    assertFalse(numbers.update(key, runOn)(_ + 1))
    assertFalse(numbers.updateIf(_ == 0, key, runOn)(_ + 1))
    insert(key, 3)
    assertFalse(numbers.updateIf(_ == 0, key, runOn)(_ + 1))
    assertTrue(numbers.update(key, runOn)(_ + 4))
    assertEquals(Some(7), get(key))
    assertEquals(Some(10), numbers.updateAndGet(key, runOn)(_ + 3))
    assertEquals(None, numbers.updateAndGetIf(_ > 100, key, runOn)(_ + 3))
    assertEquals(Some(14), numbers.updateAndGetIf(_ < 100, key, runOn)(_ + 4))
    assertTrue(numbers.updateIf(_ == 14, key, runOn)(_ - 4))
    assertEquals(Some(10), numbers.getAndUpdate(key, runOn)(_ + 17))
    assertEquals(Some(27), get(key))
    assertEquals(Some(27, false), numbers.getAndUpdateIf(_ > 100, key, runOn)(_ - 4))
    assertEquals(Some(27, true), numbers.getAndUpdateIf(_ < 100, key, runOn)(_ - 4))
    assertEquals(Some(23), get(key))
    remove(key)
    assertEquals(None, numbers.getAndUpdate(key, runOn)(_ + 2))
    assertEquals(None, get(key))
  }
}
