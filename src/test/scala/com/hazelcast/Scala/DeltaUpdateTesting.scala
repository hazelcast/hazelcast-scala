package com.hazelcast.Scala

import java.util.UUID

import org.scalatest.Assertions._
import com.hazelcast.core.IExecutorService

object DeltaUpdateTesting {

  type Map = KeyedDeltaUpdates[UUID, Int] { type UpdateR[T] = T }

  def testUpsert(numbers: Map, get: UUID => Option[Int], remove: UUID => Unit, runOn: IExecutorService = null) {
    val key = UUID.randomUUID()
    numbers.upsert(key, 5, runOn)(_ + 1) match {
      case WasUpdated => fail("Should have been Insert")
      case WasInserted => assert(get(key) == Some(5))
    }
    numbers.upsert(key, 3, runOn)(_ + 9) match {
      case WasInserted => fail("Should have been Update")
      case WasUpdated => assert(get(key) == Some(14))
    }
    assert(numbers.upsertAndGet(key, 7, runOn)(_ + 6) == 20)
    assert(numbers.getAndUpsert(key, 11, runOn)(_ + 3) == Some(20))
    assert(get(key) == Some(23))
    remove(key)
    assert(numbers.getAndUpsert(key, 45, runOn)(_ + 2) == None)
    assert(get(key) == Some(45))
    assert(numbers.upsertAndGet(UUID.randomUUID, 99, runOn)(_ + 55) == 99)
  }

  def testUpdate(numbers: Map, get: UUID => Option[Int], insert: (UUID, Int) => Unit, remove: UUID => Unit, runOn: IExecutorService = null) {
    val key = UUID.randomUUID()
    assert(numbers.updateAndGet(key, runOn)(_ + 3) == None)
    assert(numbers.updateAndGetIf(_ => true, key, runOn)(_ + 3) == None)
    assert(numbers.getAndUpdateIf(_ => true, key, runOn)(_ + 3) == None)
    assert(!numbers.update(key, runOn)(_ + 1))
    assert(!numbers.updateIf(_ == 0, key, runOn)(_ + 1))
    insert(key, 3)
    assert(!numbers.updateIf(_ == 0, key, runOn)(_ + 1))
    assert(numbers.update(key, runOn)(_ + 4))
    assert(get(key) == Some(7))
    assert(numbers.updateAndGet(key, runOn)(_ + 3) == Some(10))
    assert(numbers.updateAndGetIf(_ > 100, key, runOn)(_ + 3) == None)
    assert(numbers.updateAndGetIf(_ < 100, key, runOn)(_ + 4) == Some(14))
    assert(numbers.updateIf(_ == 14, key, runOn)(_ - 4))
    assert(numbers.getAndUpdate(key, runOn)(_ + 17) == Some(10))
    assert(get(key) == Some(27))
    assert(numbers.getAndUpdateIf(_ > 100, key, runOn)(_ - 4) == Some((27, false)))
    assert(numbers.getAndUpdateIf(_ < 100, key, runOn)(_ - 4) == Some((27, true)))
    assert(get(key) == Some(23))
    remove(key)
    assert(numbers.getAndUpdate(key, runOn)(_ + 2) == None)
    assert(get(key) == None)
  }
}
