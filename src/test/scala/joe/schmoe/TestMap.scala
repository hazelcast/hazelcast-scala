package joe.schmoe

import java.util.Date
import java.util.Map.Entry
import java.util.StringTokenizer
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import scala.BigDecimal.RoundingMode._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.util._

import org.scalatest._, Matchers._

import com.hazelcast.Scala._
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapIndexConfig
import com.hazelcast.core.IMap
import com.hazelcast.map.AbstractEntryProcessor
import com.hazelcast.query.Predicate
import com.hazelcast.core.MapStore
import scala.util.control.NoStackTrace
import com.hazelcast.core.IExecutorService

object TestMap extends ClusterSetup {
  override val clusterSize = 3
  def init: Unit = {
    TestKryoSerializers.register(memberConfig.getSerializationConfig)
    TestKryoSerializers.register(clientConfig.getSerializationConfig)
    memberConfig.getSerializationConfig.setAllowUnsafe(true)
    clientConfig.getSerializationConfig.setAllowUnsafe(true)
  }
  def destroy = ()
  case class MyNumber(num: Int)
}

class TestMap extends FunSuite with CleanUp with BeforeAndAfterAll with BeforeAndAfter {

  import TestMap._

  def hzs = TestMap.hzs

  override def beforeAll = beforeClass()
  override def afterAll = afterClass()

  after {
    cleanup
  }

  test("local access should be fast") {
    assert(HackIntegrityTesting.verifyFastAccess(member.getMap(randName)))
  }

  test("upsert should work") {
    val map = client.getMap[UUID, Int](randName)
    DeltaUpdateTesting.testUpsert(map, key => Option(map.get(key)), key => map.remove(key))
    map.clear()
    val exec = client.getExecutorService("default")
    DeltaUpdateTesting.testUpsert(map, key => Option(map.get(key)), key => map.remove(key), exec)
  }

  test("asyncPutIfAbsent should work") {
    val map = client.getMap[Int, String](randName)
    val firstPut = map.async.putIfAbsent(5, "Hello").await
    assert(firstPut == None)
    val secondPut = map.async.putIfAbsent(5, "World").await
    assert(secondPut == Some("Hello"))
    val thirdPut = map.async.putIfAbsent(5, "Doh!", 2.seconds).await
    assert(thirdPut == Some("Hello"))
    val removed = map.async.remove(5).await
    assert(removed == Some("Hello"))
    val fourthPut = map.async.putIfAbsent(5, "TTL", 1.second).await
    assert(fourthPut == None)
    assert(map.get(5) == "TTL")
    Thread sleep 1000
    assert(null == map.get(5))
  }

  test("asyncSet should work") {
      def asyncSet(map: IMap[Int, String]): Unit = {
        map.async.set(5, "Hello").await
        assert("Hello" == map.get(5))
        map.async.set(5, "World").await
        assert("World" == map.get(5))
        map.async.set(5, "Doh!", 10.seconds).await
        assert("Doh!" == map.get(5))
        map.async.set(5, "TTL", 1.second).await
        Thread sleep 1000
        assert(null == map.get(5))
      }
    asyncSet(client.getMap[Int, String](randName))
    asyncSet(member.getMap[Int, String](randName))
  }

  test("asyncUpdateIfPresent") {
    val map = client.getMap[String, Int](randName)
    val fi = map.async.update("foo")(_ + 1)
    val latch = new CountDownLatch(1)
    fi.onComplete {
      case Success(wasUpdated) =>
        assert(!wasUpdated)
        latch.countDown()
      case Failure(t) => t.printStackTrace()
    }
    assert(latch.await(10, TimeUnit.SECONDS))
  }
  test("update") {
    val map = client.getMap[UUID, Int](randName)
      def moreTests(runOn: IExecutorService = null): Unit = {
        (1 to 10) foreach { _ =>
          map.set(UUID.randomUUID, 5)
        }
        map.updateAll()(_ + 6)
        val values = map.values
        assert(values.size == 10)
        values.asScala.foreach { value =>
          assert(value == 11)
        }
        val updated = map.updateAndGetAll(where.value = 11)(_ - 12)
        assert(updated.size == 10)
        updated.values.foreach { value =>
          assert(value == -1)
        }
      }
    DeltaUpdateTesting.testUpdate(map, key => Option(map.get(key)), map.put(_, _), key => map.remove(key))
    map.clear()
    moreTests()
    val exec = client.getExecutorService("default")
    DeltaUpdateTesting.testUpdate(map, key => Option(map.get(key)), map.put(_, _), key => map.remove(key), exec)
    map.clear()
    moreTests(exec)
  }
  test("asyncUpdateWithDefault") {
    val map = client.getMap[String, Int](randName)
    val fi = map.async.upsertAndGet("foo", 1)(_ + 1)
    val latch = new CountDownLatch(2)
    fi.onComplete {
      case Success(updated) =>
        assert(updated == 1)
        latch.countDown()
        map.async.upsertAndGet("foo", 1)(_ + 1).onComplete {
          case Success(updated) =>
            assert(updated == 2)
            latch.countDown()
          case Failure(t) => t.printStackTrace()
        }
      case Failure(t) => t.printStackTrace()
    }
    assert(latch.await(10, TimeUnit.SECONDS))
  }
  test("syncUpdateWithDefault") {
    val map = client.getMap[String, Int](randName)
    val latch = new CountDownLatch(4)
    val reg = map.filterKeys("foo").onEntryEvents() {
      case EntryAdded(key, value) =>
        assert(key == "foo")
        assert(value == 1)
        latch.countDown()
      case EntryUpdated(key, oldValue, newValue) =>
        assert(key == "foo")
        assert(oldValue == 1)
        assert(newValue == 2)
        latch.countDown()
    }
    val callback = new OnEntryAdded[String, Int] with OnEntryUpdated[String, Int] {
      def apply(evt: EntryAdded[String, Int]): Unit = {
        assert(evt.key == "foo")
        assert(evt.value == 1)
        latch.countDown()
      }
      def apply(evt: EntryUpdated[String, Int]): Unit = {
        assert(evt.key == "foo")
        assert(evt.oldValue == 1)
        assert(evt.newValue == 2)
        latch.countDown()
      }
    }
    val cbReg = map.filterKeys("foo").onEntryEvents(callback)

    assert(map.upsertAndGet("foo", 1)(_ + 1) == 1)
    assert(map.upsertAndGet("bar", 1)(_ + 1) == 1)
    assert(map.upsertAndGet("foo", 1)(_ + 1) == 2)
    assert(map.upsertAndGet("bar", 1)(_ + 1) == 2)
    val latchCompleted = latch.await(50000, TimeUnit.SECONDS)
    assert(latch.getCount == 0)
    assert(latchCompleted)
    reg.cancel()
    cbReg.cancel()
  }

  test("entryTest") {
    val clientMap = client.getMap[String, Int](randName)
    entryTest(clientMap)
    clientMap.clear()
    entryTest(member.getMap[String, Int](clientMap.getName))
  }
  private def entryTest(map: IMap[String, Int]): Unit = {
    val isFactor37 = (c: Int) => c % 37 == 0
    val count = 50000
    val tempMap = new java.util.HashMap[String, Int](count)
    for (i <- 1 to count) {
      tempMap.put("key" + i, i)
    }
    map.putAll(tempMap)
    val all = map.execute(OnEntries()) { entry =>
      None
    }
    assert(all.keySet == tempMap.keySet.asScala)
    val predicate37 = new Predicate[String, Int] {
      def apply(entry: Entry[String, Int]) = isFactor37(entry.value)
    }
    val entryProcessor = new AbstractEntryProcessor[String, Int](false) {
      def process(entry: Entry[String, Int]): Object = {
        (entry.value * 2).asInstanceOf[Object]
      }
    }
    val result1a = map.executeOnEntries(entryProcessor, predicate37).asScala.asInstanceOf[collection.mutable.Map[String, Int]]
    val verifyFactor37 = result1a.values.forall(obj => isFactor37(obj.asInstanceOf[Int] / 2))
    assert(verifyFactor37)
    val result1b = map.execute(OnValues(v => isFactor37(v))) { entry =>
      entry.value * 2
    }
    assert(result1b == result1a)
    val result2a = map.execute(OnValues(isFactor37)) { entry =>
      entry.value * 2
    }
    val result2b = map.map(_.value).collect {
      case value if isFactor37(value) => value * 2
    }.values().await.sorted
    val result2c = map.map(_.value).filter(isFactor37).collect {
      case value => value * 2
    }.values().await.sorted
    val result2d = map.filter((k, v) => isFactor37(v)).map(_.value * 8).map(_ / 4).values().await.sorted
    assert(result2a == result1a)
    assert(result2b == result1a.values.toSeq.sorted)
    assert(result2c == result2b)
    assert(result2d == result2c)
    val thirtySeven = 37
    val result3a = map.executeOnEntries(entryProcessor, where.value = thirtySeven)
    assert(result3a.get("key37") == 37 * 2)
    val result3b = map.execute(OnValues(_ == 37))(entry => entry.value * 2)
    val result3c = map.query(where.value = 37)(_ * 2)
    assert(result3b("key37") == result3a.get("key37"))
    assert(result3c == result3b)
  }

  test("divByZero") {
    val map = client.getMap[String, Int](randName)
    map.put("foo", 42)
    assert(map.update("foo")(_ - 2))
    assert(40 == map.get("foo"))
    assertThrows[ArithmeticException] {
      map.update("foo")(_ / 0)
    }
  }

  test("summation") {
    val map = client.getMap[String, MyNumber](randName)
    for (i <- 1 to 200) {
      val n = MyNumber(i)
      map.set(n.toString, n)
    }
    val map1to100 = map.map(_.value.num).filter(_ <= 100)
    val sum = map1to100.sum().await
    assert(sum == 5050)
    assert(map1to100.sum().await == sum)
  }

  test("min & max") {
    val map = client.getMap[String, Long](randName)
    assert(map.map(_.value).minMax.await == None)
    for (n <- 1L to 500L) {
      map.set(n.toString, n)
    }
    val filtered = map.filter(where.value.between(50, 99)).map(_.value)
    val (min, max) = filtered.minMax().await.get
    assert(min == 50L)
    assert(max == 99L)
    val filteredEvenMore = filtered.filter(_ < 60)
    val min50 = filteredEvenMore.min().await
    val max59 = filteredEvenMore.max().await
    assert(min50 == Some(50L))
    assert(max59 == Some(59L))
    assert(filtered.range().await == Some(49))
    assert(filteredEvenMore.range().await == Some(9))
  }
  test("mean") {
    val map = client.getMap[String, Int](randName)
    assert(map.map(_.value).mean().await.isEmpty)
    for (n <- 1 to 4) map.set(n.toString, n)
    val intAvg = map.map(_.value).mean.await.get
    assert(intAvg == 2)
    val dblAvg = map.map(_.value.toDouble).mean.await.get
    assert(dblAvg === 2.5 +- 0.00001)
  }

  test("distribution") {
    val iterations = 100
    val memberMap = member.getMap[Int, String](randName)
    val clientMap = client.getMap[Int, String](randName)
    for (i <- 1 to iterations) {
      distribution(clientMap, i == iterations)
      distribution(memberMap, i == iterations)
    }
      def distribution(map: IMap[Int, String], printTimings: Boolean): Unit = {
        val localMap = new java.util.HashMap[Int, String]
        (1 to 1000).foreach { n =>
          val fizzBuzz = new StringBuilder()
          if (n % 3 == 0) fizzBuzz ++= "Fizz"
          if (n % 5 == 0) fizzBuzz ++= "Buzz"
          if (fizzBuzz.isEmpty) fizzBuzz ++= n.toString
          localMap.put(n, fizzBuzz.result)
        }
        map.putAll(localMap)
        val (distribution, distrTime) = timed(unit = MICROSECONDS) { map.filterKeys(_ <= 100).map(_.value).distribution().await }
        val (distinct, distcTime) = timed(unit = MICROSECONDS) { map.filter(where.key <= 100).map(_.value).distinct().await }
        assert(distinct == distribution.keySet)
        assert(distinct == distribution.keySet)
        if (printTimings) println(s"Distribution: $distrTime μs, Distinct: $distcTime μs, using ${map.getClass.getSimpleName}")
        assert(distribution("Fizz") == 27)
        assert(distribution("Buzz") == 14)
        assert(distribution("FizzBuzz") == 6)
        assert(distribution.filterKeys(!Set("Fizz", "Buzz", "FizzBuzz").contains(_)).forall(_._2 == 1))
        val top1 = map.filterKeys(_ <= 100).map(_.value).distribution().map { dist =>
          dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(1).next
        }.await
        assert(top1 == 27 -> Set("Fizz"))
        val top3 = map.filter(where.key <= 100).map(_.value).distribution().map { dist =>
          dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(3).toMap
        }.await
        assert(top3 == Map(27 -> Set("Fizz"), 14 -> Set("Buzz"), 6 -> Set("FizzBuzz")))
      }
  }

  test("sql pred") {
    val map = client.getMap[UUID, Employee](randName)
    (1 to 1000) foreach { i =>
      val emp = Employee.random
      map.set(emp.id, emp)
    }
    val sqlPred = new com.hazelcast.query.SqlPredicate("active AND ( age > 20 OR salary < 60000 )")
    val sqlResult = map.values(sqlPred)
    val whereResult = map.values((where("active") = true) && (where("age") > 20 || where("salary") < 60000))
    assert(whereResult.asScala == sqlResult.asScala)
    val sqlPredFirst = sqlResult.asScala.head
    val firstByKey = map.get(sqlPredFirst.id)
    assert(firstByKey == sqlPredFirst)
    val sqlSalaries = sqlResult.asScala.map(e => e.id -> e.salary).toMap
    val querySalaryResult = map.query(sqlPred)(_.salary)
    assert(querySalaryResult == sqlSalaries)
    val ages = Seq[Comparable[Integer]](30, 40, 50)
    val anniversary1 = map.values(where("age") in ages).asScala
    assert(anniversary1.nonEmpty)
    anniversary1.foreach { emp =>
      assert(ages.contains(emp.age))
    }
    val anniversary2 = map.values(where("age") in (30, 40, 50)).asScala
    assert(anniversary2 == anniversary1)
    val anniversary3 = map.values(where("age") in (ages: _*)).asScala
    assert(anniversary3 == anniversary1)
  }

  test("getAs") {
    val map = client.getMap[UUID, Employee](randName)
    val emp = Employee.random
    map.set(emp.id, emp)
    val age = map.getAs(emp.id)(_.age).get
    assert(age == emp.age)
    val emp2 = Employee.random
    map.set(emp2.id, emp2)
    val salaries = map.getAllAs(Set(emp.id, emp2.id))(_.salary)
    assert(salaries(emp.id) == emp.salary)
    assert(salaries(emp2.id) == emp2.salary)
  }

  test("large map test") {
    val OneThousand = 1000
    val Thousands = 125

    val mapName = UUID.randomUUID.toString
    memberConfig.getMapConfig(mapName)
      .addMapIndexConfig(new MapIndexConfig("salary", true))
      .setInMemoryFormat(InMemoryFormat.OBJECT)
    val clientMap = client.getMap[UUID, Employee](mapName)

    var allSalaries = 0L
    var empCount = 0
    var minSal = Int.MaxValue
    var maxSal = Int.MinValue
    (1 to Thousands).foreach { _ =>
      val localMap = new java.util.HashMap[UUID, Employee]
      (1 to OneThousand).foreach { _ =>
        val emp = Employee.random
        localMap.put(emp.id, emp)
        allSalaries += emp.salary
        empCount += 1
        minSal = minSal min emp.salary
        maxSal = maxSal max emp.salary
      }
      clientMap.putAll(localMap)
    }
    assert(empCount == Thousands * OneThousand)

    val (inlineStats, inlineStatsMs) = timed(warmups = 3) {
      clientMap.aggregate(new Stats())({
        case (stats, entry) => stats.update(entry.value.salary)
      }, {
        case (x, y) => x.mergeWith(y)
      }).await
    }
    assert(inlineStats.count == empCount)
    assert(inlineStats.sum == allSalaries)
    assert(inlineStats.min == minSal)
    assert(inlineStats.max == maxSal)
    println(s"Mutable inline stats took $inlineStatsMs ms")

    val localAvg = allSalaries / empCount
    val (inlineAvg, inlineAvgMs) = timed(warmups = 3) {
      clientMap.aggregate(0L -> 0)({
        case ((sum, count), entry) =>
          (sum + entry.value.salary) -> (count + 1)
      }, {
        case ((xSum, xCount), (ySum, yCount)) => (xSum + ySum) -> (xCount + yCount)
      }).map(sc => sc._1 / sc._2).await
    }
    assert(inlineAvg == localAvg)
    val (avg, avgMs) = timed(warmups = 3) {
      clientMap.map(_.value.salary.toLong).mean.await.get
    }
    assert(avg == localAvg)
    println(s"Average salary : $$$avg")
    val (mrAvg, mrMs) = timed(warmups = 1) {
      import com.hazelcast.mapreduce.aggregation._
      val supplySalaryForAll = new Supplier[UUID, Employee, Long] {
        def apply(entry: Entry[UUID, Employee]): Long = entry.value.salary.toLong
      }
      val averageAggr = Aggregations.longAvg.asInstanceOf[Aggregation[UUID, Long, Long]]
      clientMap.aggregate(supplySalaryForAll, averageAggr)
    }
    assert(mrAvg == avg)
    println(s"Aggregation timings: $avgMs ms (Scala built-in), $inlineAvgMs ms (Scala inline), $mrMs ms (Map/Reduce), factor: ${mrMs / avgMs.toFloat}")
    val (fCount, fCountMs) = timed()(clientMap.filterValues(_.salary > 495000).count.await) // Function filter
    val (cCount, cCountMs) = timed()(clientMap.filter(where("salary") > 495000).count.await) // Predicate filter
    println(s"Filter: $fCount employees make more than $$495K ($fCountMs ms)")
    println(s"Predicate: $cCount employees make more than $$495K ($cCountMs ms) <- Predicates are faster with indexing.")

    val (javaPage, ppTime) = timed() {
      clientMap.values(20 until 40)(sortBy = _.value.salary, reverse = true)
    }
    val (scalaPage, scalaTime) = timed(warmups = 4) { clientMap.map(_.value).sortBy(_.salary).reverse.drop(20).take(20).values().await }
    println(s"Paging timings: $scalaTime ms (Scala), $ppTime ms (PagingPredicate), factor: ${ppTime / scalaTime.toFloat}")
    assert(javaPage.size == 20)
    assert(scalaPage.size == javaPage.size)
    val javaSal = javaPage.map(_.salary).toIndexedSeq
    val scalaSal = scalaPage.map(_.salary)
    println(s"First salary: $$${scalaSal(0)} (Scala), $$${javaSal(0)} (Java)")
    (0 until javaSal.size).foreach { idx =>
      assert(idx -> scalaSal(idx) == idx -> javaSal(idx))
    }

  }

  test("median") {
    val strMap = client.getMap[String, String](randName)
    val strValues = strMap.map(_.value)
    assert(strValues.medianValues.await == None)
    strMap.set("a", "a") // 1:[0:"a"]
    assert(strValues.medianValues.await == Some(("a", "a")))
    strMap.set("b", "b") // 2:[0:"a", 1:"b"]
    assert(strValues.medianValues.await == Some(("a", "b")))
    strMap.set("c", "c") // 3:[0:"a", 1:"b", 2:"c"]
    assert(strValues.medianValues.await == Some(("b", "b")))
    strMap.set("d", "d") // 4:[0:"a", 1:"b", 2:"c", 3:"d"]
    assert(strValues.medianValues.await == Some(("b", "c")))
    strMap.set("e", "e") // 5:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e"]
    assert(strValues.medianValues.await == Some(("c", "c")))
    strMap.set("e2", "e") // 6:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"e"]
    assert(strValues.medianValues.await == Some(("c", "d")))
    strMap.set("e3", "e") // 7:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"e", 6:"e"]
    assert(strValues.medianValues.await == Some(("d", "d")))
    strMap.set("b2", "b") // 8:[0:"a", 1:"b", 2:"b", 3:"c", 4:"d", 5:"e", 6:"e", 7:"e"]
    assert(strValues.medianValues.await == Some(("c", "d")))
    strMap.set("c2", "c") // 9:[0:"a", 1:"b", 2:"b", 3:"c", 4:"c", 5:"d", 6:"e", 7:"e", 8:"e"]
    assert(strValues.medianValues.await == Some(("c", "c")))
    strMap.set("c3", "c") // 10:[0:"a", 1:"b", 2:"b", 3:"c", 4:"c", 5:"c", 6:"d", 7:"e", 8:"e", 9:"e"]
    assert(strValues.medianValues.await == Some(("c", "c")))
    strMap.delete("a") // 9:[0:"b", 1:"b", 2:"c", 3:"c", 4:"c", 5:"d", 6:"e", 7:"e", 9:"e"]
    assert(strValues.medianValues.await == Some(("c", "c")))
    strMap.delete("b2") // 8:[0:"b", 1:"c", 2:"c", 3:"c", 4:"d", 5:"e", 6:"e", 7:"e"]
    assert(strValues.medianValues.await == Some(("c", "d")))
    strMap.set("d2", "d") // 9:[0:"b", 1:"c", 2:"c", 3:"c", 4:"d", 5:"d", 6:"e", 7:"e", 8:"e"]
    assert(strValues.medianValues.await == Some(("d", "d")))
    strMap.set("c4", "c") // 10:[0:"b", 1:"c", 2:"c", 3:"c", 4:"c", 5:"d", 6:"d", 7:"e", 8:"e", 9:"e"]
    assert(strValues.medianValues.await == Some(("c", "d")))

    val intMap = client.getMap[String, Int](randName)
    val intValues = intMap.map(_.value)
    assert(intValues.median().await == None)
    intMap.set("1", 1)
    assert(intValues.median().await == Some(1))
    intMap.set("2", 2)
    assert(intValues.map(_.toFloat).median().await == Some(1.5f))
    intMap.set("3", 3)
    assert(intValues.map(_.toFloat).median().await == Some(2f))
    intMap.set("4", 4)
    assert(intValues.map(_.toFloat).median().await == Some(2.5f))
    intMap.set("5", 5)
    assert(intValues.map(_.toFloat).median().await == Some(3f))
    intMap.set("6", 6)
    assert(intValues.map(_.toFloat).median().await == Some(3.5f))

    strMap.clear()
    strMap.set("a", "ABC")
    assert(strValues.medianValues.await == Some(("ABC", "ABC")))
    strMap.set("b", "ABC")
    assert(strValues.medianValues.await == Some(("ABC", "ABC")))
    strMap.set("c", "ABC")
    assert(strValues.medianValues.await == Some(("ABC", "ABC")))
    strMap.set("d", "ABC")
    assert(strValues.medianValues.await == Some(("ABC", "ABC")))
  }

  test("mode") {
    type X = Int
    type Y = Int
    val localMap = new java.util.HashMap[X, Y]
    (1 to 100) foreach { x =>
      val y = Random.nextInt(10)
      localMap.put(x, y)
    }
    val map = client.getMap[X, Y](randName)
    map.putAll(localMap)
    val values = map.map(_.value)
    val dist = values.distribution.await
    var localModeFreq: Int = -1
    var localModeValues = Set.empty[Y]
    dist.foreach {
      case (y, freq) =>
        val localFreq = localMap.values.asScala.count(_ == y)
        assert(freq == localFreq)
        if (freq > localModeFreq) {
          localModeFreq = freq
          localModeValues = Set(y)
        } else if (freq == localModeFreq) {
          localModeValues += y
        }
    }
    val valueSet = values.mode.await
    val (freq, freqValues) = values.distribution().map { dist =>
      dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(1).next
    }.await
    assert(valueSet == localModeValues)
    assert(freqValues.nonEmpty)
    assert(freq == localModeFreq)
    assert(freqValues == localModeValues)
    println(s"Value(s) `${freqValues.mkString(",")}` occurred $freq times")
    freqValues.foreach { value =>
      assert(dist(value) == freq)
    }
  }

  test("wordcount: alice in wonderland") {
    val WordFinder = """(\w|(?:'s)|(?:'t))+""".r
    val aliceChapters = client.getMap[String, String]("alice")
    val aliceSrc = Source.fromInputStream(getClass.getResourceAsStream("/alice.txt"), "UTF-8")
    val sb = new java.lang.StringBuilder
    var currChapter: Option[String] = None
      def saveChapter(): Unit = {
        currChapter.foreach { chapter =>
          aliceChapters.set(chapter, sb.toString)
        }
        sb.setLength(0)
      }
    aliceSrc.getLines.foreach { line =>
      if (line startsWith "CHAPTER") {
        saveChapter()
        currChapter = Some(line)
      } else {
        sb append line append "\n"
      }
    }
    saveChapter()
    assert(aliceChapters.size == 12)
    val words = aliceChapters.map(_.value).flatMap { chapter =>
      WordFinder.findAllMatchIn(chapter.toLowerCase).map(_.matched).toTraversable
    }
    val expectedTopTen = Map(1615 -> Set("the"), 870 -> Set("and"), 724 -> Set("to"), 627 -> Set("a"), 542 -> Set("i"), 541 -> Set("she"), 538 -> Set("it"), 513 -> Set("of"), 462 -> Set("said"), 411 -> Set("you"))
    val (top10ByCount, countMs) = timed() {
      val countByWord = words.groupBy().count().await
      val foo = countByWord.groupBy(_._2)
      foo.mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(10).toMap
    }
    assert(top10ByCount == expectedTopTen)
    println(s"Top 10 words took $countMs ms")

    val longestWord = words.maxBy(_.length).await.get
    println(s""""Alice in Wonderland", longest word: $longestWord""")
    val top5LongestWords = words.sortBy(_.length).reverse.take(5).values.await
    assert(top5LongestWords contains longestWord)
    val topStr = top5LongestWords.map(w => s"$w (${w.length})").mkString(", ")
    println(s""""Alice in Wonderland", top 5 longest words: $topStr""")
  }

  test("wordcount: flatland") {
    val flatlandChapters = client.getMap[String, String]("flatland")
    val flatlandSrc = Source.fromInputStream(getClass.getResourceAsStream("/flatland.txt"), "UTF-8")
    val sb = new java.lang.StringBuilder
    var currChapter: String = "PREFACE"
      def saveChapter(): Unit = {
        flatlandChapters.set(currChapter, sb.toString)
        sb.setLength(0)
      }
    flatlandSrc.getLines.map(_.trim).foreach { line =>
      if (line startsWith "Section ") {
        saveChapter()
        currChapter = line
      } else {
        sb append line append "\n"
      }
    }
    saveChapter()
    val tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']"
    val lines = flatlandChapters.map(e => s"${e.key}\n${e.value}").flatMap(_.split("\n+")).filter(s => s.trim.length > 0)
    val words = lines.flatMap { line =>
      val cleanLine = line.toLowerCase.replaceAll(tokens, " ")
      val tokenizer = new StringTokenizer(cleanLine)
      tokenizer.asScala.map(_.toString.trim).filter(_.length > 0).toSeq
    }
    val topTwenty: Map[String, Int] = words.distribution.await.toSeq.sortBy(_._2).reverseIterator.take(20).toMap
    assert(topTwenty.size == 20)
    // http://www.slideshare.net/andreaiacono/mapreduce-34478449/24
    assert(topTwenty("the") == 2286)
    assert(topTwenty("of") == 1634)
    assert(topTwenty("and") == 1098)
    assert(topTwenty("to") == 1088)
    assert(topTwenty("a") == 936)
    assert(topTwenty("i") == 735)
    assert(topTwenty("in") == 713)
    assert(topTwenty("that") == 499)
    assert(topTwenty("is") == 429)
    assert(topTwenty("you") == 419)
    assert(topTwenty("my") == 334)
    assert(topTwenty("it") == 330)
    assert(topTwenty("as") == 322)
    assert(topTwenty("by") == 317)
    assert(topTwenty("not") == 317)
    assert(topTwenty("or") == 299)
    assert(topTwenty("but") == 279)
    assert(topTwenty("with") == 273)
    assert(topTwenty("for") == 267)
    assert(topTwenty("be") == 252)

    val longestWord = words.maxBy(_.length).await.get
    println(s""""Flatland", longest word: $longestWord""")
    val top5LongestWords = words.sortBy(_.length).reverse.take(5).values.await
    assert(top5LongestWords contains longestWord)
    val topStr = top5LongestWords.map(w => s"$w (${w.length})").mkString(", ")
    println(s""""Flatland", top 5 longest words: $topStr""")

  }

  test("mean max temp per month") {
    val dateFmt = new java.text.SimpleDateFormat("dd/MM/yyyy")
    val milanData = Source.fromInputStream(getClass.getResourceAsStream("/milan-weather.csv"), "UTF-8")
    val localWeather = new java.util.HashMap[Date, Weather]
    milanData.getLines.foreach { daily =>
      val fields = daily.split("[,\\s]+")
      val date = dateFmt.parse(fields(0))
      val min = fields(1).toFloat
      val max = fields(2).toFloat
      localWeather.put(date, Weather(min, max))
    }
    val milanWeather = client.getMap[Date, Weather](randName)
    milanWeather.putAll(localWeather)
    val monthYearMax = milanWeather.map { entry =>
      val yearMonthFmt = new java.text.SimpleDateFormat("MMyyyy")
      val date = entry.key
      val yearMonth = yearMonthFmt.format(date)
      yearMonth -> entry.value.tempMax
    }
    val byMonthYear = monthYearMax.groupBy(_._1, _._2)
    val maxMeanByMonth = byMonthYear.mean.await
    val err = 0.005f

    // http://www.slideshare.net/andreaiacono/mapreduce-34478449/39
    assert(maxMeanByMonth("022012") === 7.23f +- err)
    assert(maxMeanByMonth("022013") === 7.2f +- err)
    assert(maxMeanByMonth("022010") === 7.85f +- err)
    assert(maxMeanByMonth("022011") === 9.79f +- err)
    assert(maxMeanByMonth("032013") === 10.74f +- err)
    assert(maxMeanByMonth("032010") === 13.13f +- err)
    assert(maxMeanByMonth("032012") === 18.55f +- err)
    assert(maxMeanByMonth("032011") === 13.74f +- err)
    assert(maxMeanByMonth("022003") === 9.28f +- err)
    assert(maxMeanByMonth("022004") === 10.41f +- err)
    assert(maxMeanByMonth("022005") === 9.15f +- err)
    assert(maxMeanByMonth("022006") === 8.9f +- err)
    assert(maxMeanByMonth("022000") === 12.34f +- err)
    assert(maxMeanByMonth("022001") === 12.16f +- err)
    assert(maxMeanByMonth("022002") === 11.84f +- err)

    val topTen = maxMeanByMonth.toSeq.sortBy(_._2).reverseIterator.take(10).toIndexedSeq.map {
      case (my, temp) => my -> (math.round(temp * 100) / 100f)
    }
    assert(topTen(0) == "082003" -> 35.29f)
    assert(topTen(1) == "062003" -> 33.04f)
    assert(topTen(2) == "072006" -> 32.71f)
    assert(topTen(3) == "082001" -> 32.38f)
    assert(topTen(4) == "072003" -> 32.04f)
    assert(topTen(5) == "072007" -> 31.91f)
    assert(topTen(6) == "082012" -> 31.81f)
    assert(topTen(7) == "082009" -> 31.68f)
    assert(topTen(8) == "072004" -> 31.27f)
    assert(topTen(9) == "072005" -> 31.25f)
  }

  test("key events") {
    val q = new LinkedBlockingQueue[String]
    val map = client.getMap[Int, String](randName)
    // Value mapping doesn't work, and probably would be weird?
    val reg = map.filterKeys(42, 43, 44).mapValues(_.toUpperCase).onEntryEvents() {
      case EntryAdded(_, value) => q offer value
    }
    map.set(42, "Hello")
    val hello = q.poll(5, TimeUnit.SECONDS)
    assert(hello == "Hello")
    map.set(999, "World")
    assert(q.poll(2, TimeUnit.SECONDS) == null)
    reg.cancel()
  }

  test("variance") {
    val bdMap = client.getMap[Int, BigDecimal](randName)
    1 to 120 foreach { n =>
      bdMap.set(n, n)
    }
    val variance = bdMap.map(_.value).variance(_ - 1).await.get
    assert(variance == BigDecimal(1210))
    val dVar = bdMap.map(_.value.doubleValue).variance(_ - 1).await.get
    assert(dVar === 1210d +- 0.0001)
    val grouped = bdMap.map(_.value).groupBy { n =>
      if (n <= 30) "a"
      else if (n <= 60) "b"
      else if (n <= 90) "c"
      else "d"
    }.variance(_ - 1).await
    assert(grouped.size == 4)
    grouped.values.foreach { variance =>
      assert(variance.setScale(1, HALF_EVEN) == BigDecimal(77.5d))
    }

    val dMap = client.getMap[String, Double](randName)
    val marks = Seq(2, 4, 4, 4, 5, 5, 7, 9).map(_.toDouble)
    val keys = marks.zipWithIndex.foldLeft(Set.empty[String]) {
      case (keys, (m, idx)) =>
        val key = s"s$idx"
        dMap.set(key, m)
        keys + key
    }
    val err = 0.000005d
    val meanDbl = dMap.filterKeys(keys).map(_.value).mean.await.get
    val meanBD = dMap.filterKeys(keys).map(e => BigDecimal(e.value)).mean.await.get
    assert(meanDbl === 5d +- err)
    assert(meanBD == BigDecimal("5"))
    val keysVariance = dMap.filterKeys(keys).map(_.value).variance().await.get
    val keysStdDev = dMap.filterKeys(keys).map(_.value).stdDev().await.get
    assert(keysStdDev === math.sqrt(keysVariance) +- err)
    assert(keysStdDev === 2d +- err)
    val keysVarianceC = dMap.filterKeys(keys).map(_.value).variance(_ - 1).await.get
    val keysStdDevC = dMap.filterKeys(keys).map(_.value).stdDev(_ - 1).await.get
    assert(keysStdDevC === math.sqrt(keysVarianceC) +- err)

    dMap.set("foo", 5d)
    val varianceSingleEntry = dMap.filterKeys("foo").map(_.value).variance(_ - 1).await.get
    assert(varianceSingleEntry === 0d +- err)
    val varianceNoEntry = dMap.filterKeys("bar").map(_.value).variance().await
    assert(varianceNoEntry == None)

    val localValues = (1 to 10000).map(_ => Random.nextInt(90) + (Random.nextDouble + 10d)).map(BigDecimal(_))
    val m = localValues.sum / localValues.size
    val localVariance = localValues.map(n => (n - m) * (n - m)).sum / localValues.size
    val numMap = localValues.foldLeft(client.getMap[UUID, BigDecimal](randName)) {
      case (map, bd) => map.set(UUID.randomUUID, bd); map
    }
    val hzVarianceDbl = numMap.map(_.value.toDouble).variance().await.get
    val hzVarianceBD = numMap.map(_.value).variance().await.get
    assert(hzVarianceBD.setScale(15, HALF_UP) == localVariance.setScale(15, HALF_UP))
    assert(hzVarianceDbl === localVariance.toDouble +- 0.00000001)
  }

  test("sorted dds") {
    val mymap = client.getMap[String, Int](randName)
    ('a' to 'z') foreach { c =>
      mymap.set(c.toString, c - 'a')
    }
    val pageSize = 3
    val descStrings = mymap.map(_.key.toUpperCase).sorted.reverse
      def skipPages(p: Int) = p * pageSize
    val descStringsP1 = descStrings.take(pageSize).values().await
    val descStringsP2 = descStrings.drop(skipPages(1)).take(pageSize).values().await
    assert(descStringsP1 == IndexedSeq("Z", "Y", "X"))
    assert(descStringsP2 == IndexedSeq("W", "V", "U"))
    val descInts = mymap.map(_.value).sorted.reverse
    val descIntsP1 = descInts.drop(skipPages(0)).take(pageSize).values().await
    assert(descIntsP1 == IndexedSeq(25, 24, 23))
    val descIntsMean = descInts.take(10).mean().await.get
    assert(descIntsMean == (25 + 24 + 23 + 22 + 21 + 20 + 19 + 18 + 17 + 16) / 10)
  }

  test("paging") {
    import java.util.Comparator
    import com.hazelcast.query.PagingPredicate

    val employees = (1 to 1000).foldLeft(client.getMap[UUID, Employee](randName)) {
      case (employees, _) =>
        val emp = Employee.random
        employees.set(emp.id, emp)
        employees
    }
//    val wellPaid = where("salary") >= 400000
    val wellPaid = where"salary >= 400000"
    type E = Entry[UUID, Employee]
    val asc = new Comparator[E] with Serializable {
      def compare(a: E, b: E): Int = a.value.salary.compareTo(b.value.salary)
    }.asInstanceOf[Comparator[Entry[Any, Any]]]
    val desc = java.util.Collections.reverseOrder(asc)
    val ppAsc = new PagingPredicate(wellPaid, asc, 10)
    ppAsc.setPage(4)
    val fromJavaAsc = employees.values(ppAsc).asScala.map(_.salary)
    assert(fromJavaAsc.size == 10)
    val ppDesc = new PagingPredicate(wellPaid, desc, 10)
    ppDesc.setPage(3)
    val fromJavaDesc = employees.values(ppDesc).asScala.map(_.salary)
    val entriesFromJavaDesc = employees.entrySet(ppDesc).iterator.asScala.toSeq.map(_.value.salary)
    assert(fromJavaDesc.size == 10)
    val fromScalaAsc = employees.values(40 until 50, wellPaid)(_.value.salary).map(_.salary)
    assert(fromScalaAsc.size == 10)
    val fromScalaDesc = employees.values(30 until 40, wellPaid)(_.value.salary, reverse = true).map(_.salary)
    val entriesFromScalaDesc = employees.entries(30 until 40, wellPaid)(_.value.salary, reverse = true).toSeq.map(_.value.salary)
    assert(fromScalaDesc.size == 10)
    assert(fromScalaAsc == fromJavaAsc)
    assert(fromScalaDesc == fromJavaDesc)
    assert(entriesFromScalaDesc == entriesFromJavaDesc)
  }

  test("stringLengthMinMax1") {
    val strMap = client.getMap[Int, String](randName)
    strMap.set(1, "abc")
    strMap.set(2, "abc")
    strMap.set(3, "abcdefghijklmnop")
    strMap.set(4, "abcdef")
    strMap.set(5, "abcxyz")
    strMap.set(6, "xyz")

    val maxByStr = strMap.map(_.value).max().await.get
    assert(maxByStr == "xyz")
    val maxByLen = strMap.map(_.value).maxBy(_.length).await.get
    assert(maxByLen == "abcdefghijklmnop")

    val longestByMax = strMap.maxBy(_.value.length).await.get
    assert(longestByMax.key == 3)
    assert(longestByMax.value == strMap.get(3))
    val longestBySort = strMap.sortBy(_.value.length).reverse.take(1).values.await.head
    assert(longestBySort.key == 3)
    assert(longestBySort.value == strMap.get(3))
  }
  test("stringLengthMinMax2") {
    type SomeKey = Long
    val localMap = (1 to 125000).foldLeft(new java.util.HashMap[SomeKey, String]) {
      case (map, key) =>
        map.put(key, randomString(500))
        map
    }
    val byLength = localMap.asScala.toSeq.groupBy(_._2.length)
    val (maxLength, withMaxLength) = byLength.toSeq.sortBy(_._1).reverse.head
    val strMap = client.getMap[SomeKey, String](randName)
    strMap.putAll(localMap)
    for (i <- -5 to 5) {
      val (bySort, bySortTime) = timed()(strMap.sortBy(_.value.length).reverse.take(1).values.await.head)
      val (byMax, byMaxTime) = timed()(strMap.maxBy(_.value.length).await.get)
      assert(bySort.value.length == maxLength)
      assert(byMax.value.length == maxLength)
      assert(withMaxLength.map(_._1).contains(bySort.key))
      assert(withMaxLength.map(_._1).contains(byMax.key))
      assert(withMaxLength.map(_._2).contains(bySort.value))
      assert(withMaxLength.map(_._2).contains(byMax.value))
      if (i >= 3) println(s"Longest string: maxBy(): $byMaxTime ms, sortBy(): $bySortTime ms")
    }
  }

  test("aggregateToMap") {
    var localSum = 0L
    var localCount = 0
    val employees = (1 to 50000).foldLeft(client.getMap[UUID, Employee](randName)) {
      case (employees, _) =>
        val emp = Employee.random
        localSum += emp.salary
        localCount += 1
        employees.set(emp.id, emp)
        employees
    }
    val (remoteSum, remoteCount) = employees.map(_.value).aggregate(0L -> 0)({
      case ((sum, count), emp) => (sum + emp.salary) -> (count + 1)
    }, {
      case (x, y) => (x._1 + y._1) -> (x._2 + y._2)
    }).await
    assert(remoteSum == localSum)
    assert(remoteCount == localCount)

    val resultMap = client.getMap[String, (Long, Int)](randName)
    val resultKey = "salarySumCount"
    resultMap.delete(resultKey)
    employees.map(_.value).aggregateInto(resultMap, resultKey)(0L -> 0)({
      case ((sum, count), emp) => (sum + emp.salary) -> (count + 1)
    }, {
      case (x, y) => (x._1 + y._1) -> (x._2 + y._2)
    }).await
    val (sum, count) = resultMap.get(resultKey)
    assert(sum == localSum)
    assert(count == localCount)

    val resultsByAge = client.getMap[Int, (Long, Int)](randName)
    val ages = employees.map(_.value).groupBy(_.age).aggregateInto(resultsByAge)(0L -> 0)({
      case ((sum, count), emp) => (sum + emp.salary) -> (count + 1)
    }, {
      case (x, y) => (x._1 + y._1) -> (x._2 + y._2)
    }).await
    assert(resultsByAge.size == ages.size)
    val (ageSum, ageCount) = ages.foldLeft(0L -> 0) {
      case ((totalSum, totalCount), age) =>
        val (sum, count) = resultsByAge.get(age)
        assert(count > 0)
        (totalSum + sum) -> (totalCount + count)
    }
    assert(ageSum == localSum)
    assert(ageCount == localCount)

  }

  test("for each") {
    hzs.foreach { hz =>
      hz.userCtx(Entries) = new collection.concurrent.TrieMap[Int, String]
    }
    val imap = client.getMap[Int, String](randName)
    (0 to 5000) foreach { i =>
      imap.set(i, (i * 37).toString)
    }
    imap.foreach(_.userCtx(Entries), where.key.between(501, 510)) {
      (ctxEntries, key, value) => ctxEntries.putIfAbsent(key, value)
    }
    val entries = hzs.map { hz =>
      hz.userCtx(Entries)
    }.reduce(_ ++ _)
    assert(entries.size == 10)
    entries.foreach {
      case (key, value) =>
        assert(imap.get(key) == value)
    }
  }

  test("on key(s)") {
    val myMap = client.getMap[String, Int](randName)
    (1 to 2500) foreach { i =>
      myMap.set(i.toString, i * 271)
    }
    val changes = new java.util.concurrent.LinkedBlockingQueue[(String, Int)]
    myMap.onEntryEvents() {
      case EntryUpdated(key, _, newValue) => assert {
        changes.offer(key -> newValue)
      }
    }

    myMap.execute(OnKey("45")) { entry =>
      entry.value = entry.value.map(_ * 2)
    }
    assert(myMap.get("45") == 45 * 271 * 2)
    assert(changes.take() == "45" -> (45 * 271 * 2))

    myMap.execute(OnKeys(Set("1", "2", "3"))) { entry =>
      entry.value = 0
    }
    assert(myMap.get("1") == 0)
    assert(myMap.get("2") == 0)
    assert(myMap.get("3") == 0)
    (1 to 3).map(_ => changes.take).toList.sortBy(_._1) match {
      case ("1", 0) :: ("2", 0) :: ("3", 0) :: Nil => // As expected
      case _ => fail(s"Changes: $changes")
    }

    val resMap = myMap.execute(OnKeys("1", "2", "3")) { entry =>
      entry.value = -1
    }
    assert(resMap.isEmpty)
    assert(myMap.get("1") == -1)
    assert(myMap.get("2") == -1)
    assert(myMap.get("3") == -1)

    myMap.execute(OnKey("1")) { entry =>
      entry.value = Some(-5)
    }
    assert(myMap.get("1") == -5)

  }

  test("shape shifter") {
      implicit def utf8 = UTF8Serializer
    val mapName = UUID.randomUUID.toString
    val stringMap = client.getBinaryMap[String, String](mapName)
    stringMap.put("hello", "hello")
    assert(stringMap.get("hello") == "hello")
    val updated = stringMap.updateAndGet("hello")(_ => "world")
    assert(updated == Some("world"))
    val foo = hzs(0).getBinaryMap[String, String](mapName)
    assert(foo.get("hello") == "world")
  }

  test("transient") {
    val mapStore = new MapStore[String, String] {
      def load(key: String) = null
      def loadAll(keys: java.util.Collection[String]) = java.util.Collections.emptyMap()
      def loadAllKeys() = null
      def delete(key: String) = throw new UnsupportedOperationException with NoStackTrace
      def deleteAll(keys: java.util.Collection[String]) = throw new UnsupportedOperationException with NoStackTrace
      def store(k: String, v: String) = throw new UnsupportedOperationException with NoStackTrace
      def storeAll(kv: java.util.Map[String, String]) = throw new UnsupportedOperationException with NoStackTrace
    }
    val name = UUID.randomUUID.toString
    hzs.foreach { hz =>
      hz.getConfig.getMapConfig(name).getMapStoreConfig.setImplementation(mapStore).setEnabled(true)
    }
    val map = member.getMap[String, String](name)
    try {
      map.set("Hello", "world")
      fail("Should throw exception")
    } catch {
      case _: UnsupportedOperationException => // Ok
    }
    map.setTransient("Hello", "world")
    map.setTransient("World", "hello", 1.second)
    assert(map.get("World") == "hello")
    Thread sleep 1111
    assert(map.get("World") == null)
    assert(map.get("Hello") == "world")
  }

  test("more async") {
    val employees = client.getMap[UUID, Employee](randName)
    1 to 1000 foreach { _ =>
      val emp = Employee.random
      val dur = if (emp.age % 2 == 0) Duration.Inf else 90.seconds
      employees.async.put(emp.id, emp, dur)
    }

    val top3 = employees.map(_.value).sortBy(_.salary).reverse.take(3).values.await
    assert(top3.size == 3)
    val top3Again = employees.async.getAll(top3.map(_.id).toSet).await
    assert(top3Again.size == 3)
    top3.foreach { emp =>
      assert(top3Again(emp.id) == emp)
    }
    val ceo = employees.async.get(top3.head.id).await.get
    assert(ceo == top3.head)
    val ceoSalary = employees.async.getAs(top3.head.id)(_.salary).await.get
    assert(ceoSalary == ceo.salary)
    val top3salaries = employees.async.getAllAs(top3.map(_.id).toSet)(_.salary).await
    assert(top3salaries.size == 3)
    top3 foreach { emp =>
      assert(top3salaries(emp.id) == emp.salary)
    }
  }

  test("update and return") {
    val employees = client.getMap[UUID, Employee](randName)
    val randEmp = Employee.random
    val key = randEmp.id
    employees.set(key, randEmp)
    val updatedEmp = employees.updateAndGet(key)(emp => emp.copy(salary = emp.salary * 2)).get
    assert(updatedEmp.salary == randEmp.salary * 2)
    val updatedSalary = employees.execute(OnKey(key)) { entry =>
      val updated = entry.value.map { emp =>
        emp.copy(salary = emp.salary * 2)
      }
      entry.value = updated
      updated.map(_.salary)
    }
    assert(updatedSalary == Some(randEmp.salary * 4))
  }
  test("add and remove by EP") {
    val employees = client.getMap[UUID, Employee](randName)
    val randEmp1 = Employee.random
    val key1 = randEmp1.id
    employees.set(key1, randEmp1)
    val key2 = UUID.randomUUID
    assert(employees.containsKey(key1))
    assert(!employees.containsKey(key2))
    val existed1 = employees.execute(OnKey(key1)) { entry =>
      val existed = entry.value.isDefined
      entry.value = None
      existed
    }
    assert(existed1)
    employees.execute(OnKey(key2)) { entry =>
      val existed = entry.value.isDefined
      entry.value = Some {
        Employee.random.copy(id = key2)
      }
      existed
    }
    assert(!employees.containsKey(key1))
    assert(employees.containsKey(key2))
  }

  test("update with init value") {
    val employees = client.getMap[UUID, Employee](randName)
    val randEmp1 = Employee.random
    val key1 = randEmp1.id
    val existed = employees.update(key1, randEmp1) { emp =>
      emp.copy(salary = emp.salary * 2)
    }
    assert(!existed)
    val emp1 = employees.get(key1)
    assert(emp1.salary / 2 == randEmp1.salary)
  }
  test("update and get with init value") {
    val employees = client.getMap[UUID, Employee](randName)
    val randEmp1 = Employee.random
    val key1 = randEmp1.id
    val emp1 = employees.updateAndGet(key1, randEmp1) { emp =>
      emp.copy(salary = emp.salary * 2)
    }
    assert(emp1.salary / 2 == randEmp1.salary)
  }

}
case object UTF8Serializer extends com.hazelcast.nio.serialization.ByteArraySerializer[String] {
  def destroy() = ()
  def getTypeId() = 500
  def write(str: String) = str.getBytes("UTF-8")
  def read(arr: Array[Byte]) = new String(arr, "UTF-8")
}

object Entries extends UserContext.Key[collection.concurrent.TrieMap[Int, String]]
