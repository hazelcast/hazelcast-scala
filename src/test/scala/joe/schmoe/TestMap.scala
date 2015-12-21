package joe.schmoe

import java.util.Date
import java.util.Map.Entry
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import scala.BigDecimal.RoundingMode._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Failure
import scala.util.Random
import scala.util.Success

import org.junit.Assert._
import org.junit.Test

import com.hazelcast.Scala._
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapIndexConfig
import com.hazelcast.core.IMap
import com.hazelcast.map.AbstractEntryProcessor
import com.hazelcast.query.Predicate

object TestMap extends ClusterSetup {
  override val clusterSize = 3
  def init {
    TestSerializers.register(clientConfig.getSerializationConfig)
    TestSerializers.register(memberConfig.getSerializationConfig)
    memberConfig.getMapConfig("employees").addMapIndexConfig(new MapIndexConfig("salary", true)).setInMemoryFormat(InMemoryFormat.OBJECT)
    memberConfig.getSerializationConfig.setAllowUnsafe(true)
    clientConfig.getSerializationConfig.setAllowUnsafe(true)
  }
  def destroy = ()

  case class MyNumber(num: Int)

  case class Id[T](uuid: UUID = UUID.randomUUID)
  type Price = BigDecimal
  type ProdId = Id[Product]
  type CustId = Id[Customer]
  type OrdId = Id[Order]
  case class Product(id: ProdId, name: String, price: Price)
  case class Customer(id: CustId, name: String)
  case class Order(id: OrdId, products: Map[ProdId, Int], customer: CustId)

}

class TestMap {

  import TestMap._

  @Test
  def upsert {
    val map = getClientMap[String, Int]()
    map.upsert("A", 42)(_ + 1) match {
      case Insert => // As expected
      case Update => fail("Should not be update")
    }
    map.upsert("A", 42)(_ + 1) match {
      case Insert => fail("Should not be insert")
      case Update => // As expected
    }
    val resultB1 = map.upsertAndGet("B", 42)(_ + 10)
    assertEquals(42, resultB1)
    val resultB2 = map.upsertAndGet("B", 42)(_ + 10)
    assertEquals(52, resultB2)
  }

  @Test
  def asyncUpdateIfPresent {
    val map = getClientMap[String, Int]()
    val fi = map.async.update("foo")(_ + 1)
    val latch = new CountDownLatch(1)
    fi.onComplete {
      case Success(wasUpdated) =>
        assertFalse(wasUpdated)
        latch.countDown()
      case Failure(t) => t.printStackTrace()
    }
    assertTrue(latch.await(10, TimeUnit.SECONDS))
  }
  @Test
  def syncUpdateIfPresent {
    val map = getClientMap[String, Int]()
    val wasUpdated = map.update("foo")(_ + 1)
    assertFalse(wasUpdated)
  }
  @Test
  def asyncUpdateWithDefault {
    val map = getClientMap[String, Int]()
    val fi = map.async.upsertAndGet("foo", 1)(_ + 1)
    val latch = new CountDownLatch(2)
    fi.onComplete {
      case Success(updated) =>
        assertEquals(1, updated)
        latch.countDown()
        map.async.upsertAndGet("foo", 1)(_ + 1).onComplete {
          case Success(updated) =>
            assertEquals(2, updated)
            latch.countDown()
          case Failure(t) => t.printStackTrace()
        }
      case Failure(t) => t.printStackTrace()
    }
    assertTrue(latch.await(10, TimeUnit.SECONDS))
  }
  @Test
  def syncUpdateWithDefault {
    val map = getClientMap[String, Int]()
    val latch = new CountDownLatch(2)
    val reg = map.filterKeys("foo").onEntryEvents() {
      case EntryAdded(key, value) =>
        assertEquals(1, value)
        latch.countDown()
      case EntryUpdated(key, oldValue, newValue) =>
        assertEquals(1, oldValue)
        assertEquals(2, newValue)
        latch.countDown()
    }
    val updated1 = map.upsertAndGet("foo", 1)(_ + 1)
    assertEquals(1, updated1)
    val updated2 = map.upsertAndGet("foo", 1)(_ + 1)
    assertEquals(2, updated2)
    assertTrue(latch.await(5, TimeUnit.SECONDS))
    reg.cancel()
  }

  @Test
  def entryTest {
    val clientMap = getClientMap[String, Int]()
    entryTest(clientMap)
    clientMap.clear()
    entryTest(getMemberMap[String, Int](clientMap.getName))
  }
  private def entryTest(map: IMap[String, Int]) {
    val isFactor37 = (c: Int) => c % 37 == 0
    val count = 50000
    val start = System.currentTimeMillis
    val tempMap = new java.util.HashMap[String, Int](count)
    for (i <- 1 to count) {
      tempMap.put("key" + i, i)
    }
    map.putAll(tempMap)
    println(s"Took ${System.currentTimeMillis - start} ms to putAll $count entries")
    val predicate37 = new Predicate[String, Int] {
      def apply(entry: Entry[String, Int]) = isFactor37(entry.value)
    }
    val entryProcessor = new AbstractEntryProcessor[String, Int](false) {
      def process(entry: Entry[String, Int]): Object = {
        (entry.value * 2).asInstanceOf[Object]
      }
    }
    val result1a = map.executeOnEntries(entryProcessor, predicate37).asScala.asInstanceOf[collection.mutable.Map[String, Int]]
    println(s"result1a: $result1a")
    val verifyFactor37 = result1a.values.forall(obj => isFactor37(obj.asInstanceOf[Int] / 2))
    assertTrue(verifyFactor37)
    val result1b = map.executeOnEntries(entryProcessor, isFactor37).asScala
    println(s"result1b: $result1b")
    val result2a = map.execute(OnValues(isFactor37)) { entry =>
      entry.value * 2
    }
    println(s"result2a: $result2a")
    val result2b = map.map(_.value).collect {
      case value if isFactor37(value) => value * 2
    }.fetch().await.sorted
    println(s"result2b: $result2b")
    val result2c = map.map(_.value).filter(isFactor37).collect {
      case value => value * 2
    }.fetch().await.sorted
    println(s"result2c: $result2c")
    val result2d = map.filter(e => isFactor37(e.value)).map(_.value * 8).map(_ / 4).fetch().await.sorted
    println(s"result2d: $result2d")
    assertEquals(result1a, result1b)
    assertEquals(result1a, result2a)
    assertEquals(result1a.values.toSeq.sorted, result2b)
    assertEquals(result2b, result2c)
    assertEquals(result2c, result2d)
    val thirtySeven = 37
    val result3a = map.executeOnEntries(entryProcessor, where"this = $thirtySeven")
    println(s"result3a: $result3a")
    assertEquals(37 * 2, result3a.get("key37"))
    val result3b = map.execute(OnValues(_ == 37))(entry => entry.value * 2)
    println(s"result3b: $result3b")
    val result3c = map.query(where.value = 37)(_ * 2)
    println(s"result3c: $result3c")
    assertEquals(result3a.get("key37"), result3b("key37"))
    assertEquals(result3b, result3c)
  }

  @Test(expected = classOf[ArithmeticException])
  def divByZero {
    val map = getClientMap[String, Int]()
    map.put("foo", 42)
    assertTrue(map.update("foo")(_ - 2))
    assertEquals(map.get("foo"), 40)
    map.update("foo")(_ / 0)
  }

  @Test
  def summation {
    val map = getClientMap[String, MyNumber]()
    for (i <- 1 to 200) {
      val n = MyNumber(i)
      map.set(n.toString, n)
    }
    val map1to100 = map.map(_.value.num).filter(_ <= 100)
    val sum = map1to100.sum().await
    assertEquals(5050, sum)
    assertEquals(sum, map1to100.sum().await)
  }

  @Test
  def `min & max` {
    val map = getClientMap[String, Long]()
    assertEquals(None, map.map(_.value).minMax.await)
    for (n <- 1L to 500L) {
      map.set(n.toString, n)
    }
    val filtered = map.filter(where.value.between(50, 99)).map(_.value)
    val (min, max) = filtered.minMax().await.get
    assertEquals(50L, min)
    assertEquals(99L, max)
    val filteredEvenMore = filtered.filter(_ < 60)
    val min50 = filteredEvenMore.min().await
    val max59 = filteredEvenMore.max().await
    assertEquals(Some(50L), min50)
    assertEquals(Some(59L), max59)
    assertEquals(Some(49), filtered.range().await)
    assertEquals(Some(9), filteredEvenMore.range().await)
  }
  @Test
  def mean {
    val map = getClientMap[String, Int]()
    assertTrue(map.map(_.value).mean().await.isEmpty)
    for (n <- 1 to 4) map.set(n.toString, n)
    val intAvg = map.map(_.value).mean.await.get
    assertEquals(2, intAvg)
    val dblAvg = map.map(_.value.toDouble).mean.await.get
    assertEquals(2.5, dblAvg, 0.00001)
  }

  @Test
  def distribution {
    val iterations = 100
    val memberMap = getMemberMap[Int, String]()
    val clientMap = getClientMap[Int, String]()
    for (i <- 1 to iterations) {
      distribution(clientMap, i == iterations)
      distribution(memberMap, i == iterations)
    }
      def distribution(map: IMap[Int, String], printTimings: Boolean) {
        val localMap = new java.util.HashMap[Int, String]
        (1 to 1000).foreach { n =>
          val fizzBuzz = new StringBuilder()
          if (n % 3 == 0) fizzBuzz ++= "Fizz"
          if (n % 5 == 0) fizzBuzz ++= "Buzz"
          if (fizzBuzz.isEmpty) fizzBuzz ++= n.toString
          localMap.put(n, fizzBuzz.result)
        }
        map.putAll(localMap)
        val (distribution, distrTime) = timed(MICROSECONDS) { map.filterKeys(_ <= 100).map(_.value).distribution().await }
        val (distinct, distcTime) = timed(MICROSECONDS) { map.filter(where.key <= 100).map(_.value).distinct().await }
        assertEquals(distribution.keySet, distinct)
        if (printTimings) println(s"Distribution: $distrTime μs, Distinct: $distcTime μs, using ${map.getClass.getSimpleName}")
        assertEquals(27, distribution("Fizz"))
        assertEquals(14, distribution("Buzz"))
        assertEquals(6, distribution("FizzBuzz"))
        assertTrue(distribution.filterKeys(!Set("Fizz", "Buzz", "FizzBuzz").contains(_)).forall(_._2 == 1))
        assertEquals(Map(27 -> Set("Fizz")), map.filterKeys(_ <= 100).map(_.value).frequency(1).await)
        assertEquals(Map(27 -> Set("Fizz"), 14 -> Set("Buzz"), 6 -> Set("FizzBuzz")), map.filterKeys(_ <= 100).map(_.value).frequency(3).await)
      }
  }

  @Test
  def `sql pred` {
    val map = getClientMap[UUID, Employee]()
    (1 to 1000) foreach { i =>
      val emp = Employee.random
      map.set(emp.id, emp)
    }
    val sqlPred = new com.hazelcast.query.SqlPredicate("active AND ( age > 20 OR salary < 60000 )")
    val sqlResult = map.values(sqlPred)
    val whereResult = map.values((where("active") = true) && (where("age") > 20 || where("salary") < 60000))
    assertEquals(sqlResult.asScala, whereResult.asScala)
    val sqlPredFirst = sqlResult.asScala.head
    val firstByKey = map.values(where.key() = sqlPredFirst.id)
    assertEquals(sqlPredFirst, firstByKey.asScala.head)
    val sqlSalaries = sqlResult.asScala.map(e => e.id -> e.salary).toMap
    val querySalaryResult = map.query(sqlPred)(_.salary)
    assertEquals(sqlSalaries, querySalaryResult)
  }

  @Test
  def `large map test` {
    val Thousands = 125
    val clientMap = getClientMap[UUID, Employee]("employees")
    var allSalaries = 0L
    var empCount = 0
    (1 to Thousands).foreach { _ =>
      val localMap = new java.util.HashMap[UUID, Employee]
      (1 to 1000).foreach { _ =>
        val emp = Employee.random
        localMap.put(emp.id, emp)
        allSalaries += emp.salary
        empCount += 1
      }
      clientMap.putAll(localMap)
    }
    assertEquals(Thousands * 1000, empCount)
    val localAvg = allSalaries / empCount
    val (avg, ms) = timed() {
      // Deliberately inefficient, testing piped mapping
      clientMap.map(_.value).map(_.salary).map(_.toLong).mean.await.get
    }
    println(s"Average salary : $$$avg ($ms ms)")
    assertEquals(localAvg, avg)
    val (fCount, fCountMs) = timed()(clientMap.filterValues(_.salary > 495000).count.await)
    val (cCount, cCountMs) = timed()(clientMap.filter(where("salary") > 495000).count.await)
    println(s"Filter: $fCount employees make more than $$495K ($fCountMs ms)")
    println(s"Predicate: $cCount employees make more than $$495K ($cCountMs ms). Predicates are faster with indexing.")
  }

  //  @Test @Ignore
  //  def `let's join maps` {
  //    val customerMap = {
  //      val map = getClientMap[CustId, Customer]("customers")
  //      List("Alice", "Bob", "Carl").foreach { name =>
  //        val c = new Customer(new CustId(), name)
  //        map.set(c.id, c)
  //      }
  //      map
  //    }
  //    val productMap = {
  //      val map = getClientMap[ProdId, Product]("products")
  //      List("Aged Cheese, 1 kg" -> 35d, "Dark Chocolate, 250 grams" -> 4.5, "Red Wine, 1 liter" -> 9.75).foreach {
  //        case (name, price) =>
  //          val p = new Product(new ProdId(), name, BigDecimal(price))
  //      }
  //      map
  //    }
  //    val orderId = new OrdId()
  //    val orderMap = {
  //      val map = getClientMap[OrdId, Order]("orders")
  //      val bobId = customerMap.filter(where"name = 'Bob'").fetch().head._1
  //      val productQtys = productMap.keySet().asScala.zipWithIndex.map {
  //        case (productId, idx) => productId -> (idx + 1) * 3
  //      }.toMap
  //      val order = Order(orderId, productQtys, bobId)
  //      map.set(order.id, order)
  //      map
  //    }
  //    val (order, customer, products) =
  //      orderMap.filterKeys(orderId)
  //        .innerJoinOne(customerMap, o => Some(o.customer))
  //        .innerJoinMany(productMap, _._1.products.keySet).collectValues {
  //          case ((order, customer), products) =>
  //            val prodQty = order.products.toSeq.map {
  //              case (prodId, qty) => products(prodId) -> qty
  //            }
  //            (order, customer, prodQty)
  //        }.fetch().values.head
  //    val avgOrderQty = orderMap.mapValues(_.products.map(_._2))
  //  }

  @Test
  def median {
    val strMap = getClientMap[String, String]()
    val strValues = strMap.map(_.value)
    assertEquals(None, strValues.medianValues.await)
    strMap.set("a", "a") // 1:[0:"a"]
    assertEquals(Some(("a", "a")), strValues.medianValues.await)
    strMap.set("b", "b") // 2:[0:"a", 1:"b"]
    assertEquals(Some(("a", "b")), strValues.medianValues.await)
    strMap.set("c", "c") // 3:[0:"a", 1:"b", 2:"c"]
    assertEquals(Some(("b", "b")), strValues.medianValues.await)
    strMap.set("d", "d") // 4:[0:"a", 1:"b", 2:"c", 3:"d"]
    assertEquals(Some(("b", "c")), strValues.medianValues.await)
    strMap.set("e", "e") // 5:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e"]
    assertEquals(Some(("c", "c")), strValues.medianValues.await)
    strMap.set("e2", "e") // 6:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"e"]
    assertEquals(Some(("c", "d")), strValues.medianValues.await)
    strMap.set("e3", "e") // 7:[0:"a", 1:"b", 2:"c", 3:"d", 4:"e", 5:"e", 6:"e"]
    assertEquals(Some(("d", "d")), strValues.medianValues.await)
    strMap.set("b2", "b") // 8:[0:"a", 1:"b", 2:"b", 3:"c", 4:"d", 5:"e", 6:"e", 7:"e"]
    assertEquals(Some(("c", "d")), strValues.medianValues.await)
    strMap.set("c2", "c") // 9:[0:"a", 1:"b", 2:"b", 3:"c", 4:"c", 5:"d", 6:"e", 7:"e", 8:"e"]
    assertEquals(Some(("c", "c")), strValues.medianValues.await)
    strMap.set("c3", "c") // 10:[0:"a", 1:"b", 2:"b", 3:"c", 4:"c", 5:"c", 6:"d", 7:"e", 8:"e", 9:"e"]
    assertEquals(Some(("c", "c")), strValues.medianValues.await)
    strMap.delete("a") // 9:[0:"b", 1:"b", 2:"c", 3:"c", 4:"c", 5:"d", 6:"e", 7:"e", 9:"e"]
    assertEquals(Some(("c", "c")), strValues.medianValues.await)
    strMap.delete("b2") // 8:[0:"b", 1:"c", 2:"c", 3:"c", 4:"d", 5:"e", 6:"e", 7:"e"]
    assertEquals(Some(("c", "d")), strValues.medianValues.await)
    strMap.set("d2", "d") // 9:[0:"b", 1:"c", 2:"c", 3:"c", 4:"d", 5:"d", 6:"e", 7:"e", 8:"e"]
    assertEquals(Some(("d", "d")), strValues.medianValues.await)
    strMap.set("c4", "c") // 10:[0:"b", 1:"c", 2:"c", 3:"c", 4:"c", 5:"d", 6:"d", 7:"e", 8:"e", 9:"e"]
    assertEquals(Some(("c", "d")), strValues.medianValues.await)

    val intMap = getClientMap[String, Int]()
    val intValues = intMap.map(_.value)
    assertEquals(None, intValues.median().await)
    intMap.set("1", 1)
    assertEquals(Some(1), intValues.median().await)
    intMap.set("2", 2)
    assertEquals(Some(1.5f), intValues.map(_.toFloat).median().await)
    intMap.set("3", 3)
    assertEquals(Some(2f), intValues.map(_.toFloat).median().await)
    intMap.set("4", 4)
    assertEquals(Some(2.5f), intValues.map(_.toFloat).median().await)
    intMap.set("5", 5)
    assertEquals(Some(3f), intValues.map(_.toFloat).median().await)
    intMap.set("6", 6)
    assertEquals(Some(3.5f), intValues.map(_.toFloat).median().await)

    strMap.clear()
    strMap.set("a", "ABC")
    assertEquals(Some(("ABC", "ABC")), strValues.medianValues.await)
    strMap.set("b", "ABC")
    assertEquals(Some(("ABC", "ABC")), strValues.medianValues.await)
    strMap.set("c", "ABC")
    assertEquals(Some(("ABC", "ABC")), strValues.medianValues.await)
    strMap.set("d", "ABC")
    assertEquals(Some(("ABC", "ABC")), strValues.medianValues.await)
  }

  @Test
  def mode {
    type X = Int
    type Y = Int
    val localMap = new java.util.HashMap[X, Y]
    (1 to 100) foreach { x =>
      val y = Random.nextInt(10)
      localMap.put(x, y)
    }
    val map = getClientMap[X, Y]()
    map.putAll(localMap)
    val values = map.map(_.value)
    val dist = values.distribution.await
    var localModeFreq: Int = -1
    var localModeValues = Set.empty[Y]
    dist.foreach {
      case (y, freq) =>
        val localFreq = localMap.values.asScala.count(_ == y)
        assertEquals(localFreq, freq)
        if (freq > localModeFreq) {
          localModeFreq = freq
          localModeValues = Set(y)
        } else if (freq == localModeFreq) {
          localModeValues += y
        }
    }
    val valueSet = values.mode.await
    val (freq, freqValues) = values.frequency(1).await.head
    assertEquals(localModeValues, valueSet)
    assertTrue(freqValues.nonEmpty)
    assertEquals(localModeFreq, freq)
    assertEquals(localModeValues, freqValues)
    println(s"Value(s) ${freqValues.mkString("[", ",", "]")} occurred $freq times")
    freqValues.foreach { value =>
      assertEquals(freq, dist(value))
    }
  }

  @Test
  def wordcount {
    val WordFinder = """(\w|(?:'s)|(?:'t))+""".r
    val aliceChapters = getClientMap[String, String]("alice")
    val aliceSrc = Source.fromInputStream(getClass.getResourceAsStream("/alice.txt"), "UTF-8")
    val sb = new java.lang.StringBuilder
    var currChapter: Option[String] = None
      def saveChapter() {
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
    assertEquals(12, aliceChapters.size)
    val words = aliceChapters.map(_.value).flatMap { chapter =>
      WordFinder.findAllMatchIn(chapter.toLowerCase).map(_.matched).toTraversable
    }
    val expectedTopTen = Map(1615 -> Set("the"), 870 -> Set("and"), 724 -> Set("to"), 627 -> Set("a"), 542 -> Set("i"), 541 -> Set("she"), 538 -> Set("it"), 513 -> Set("of"), 462 -> Set("said"), 411 -> Set("you"))
    val topTen = words.frequency(top = 10).await
    assertEquals(expectedTopTen, topTen)
    println(topTen)
  }

  @Test
  def `mean max temp per month` {
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
    val milanWeather = getClientMap[Date, Weather]()
    milanWeather.putAll(localWeather)
    val monthYearMax = milanWeather.map { entry =>
      val yearMonthFmt = new java.text.SimpleDateFormat("yyyy-MM")
      val date = entry.key
      val yearMonth = yearMonthFmt.format(date)
      yearMonth -> entry.value.tempMax
    }
    val byMonthYear = monthYearMax.groupBy(_._1, _._2)
    val top10Months = monthYearMax.sortBy(_._2).reverse.take(10).fetch()
    println(top10Months)
    val maxMeanByMonth = byMonthYear.mean().await
//    val foo = maxMeanByMonth.map { maxMeanByMonth =>
//      maxMeanByMonth.toSeq.sortBy(_._2).reverseIterator.take(10)
//    }

//    println(foo)
//    println(s"Milan monthly mean max temp: $ms ms")
    val err = 0.005f
    assertEquals(7.23f, maxMeanByMonth("2012-02"), err)
    assertEquals(7.2f, maxMeanByMonth("2013-02"), err)
    assertEquals(7.85f, maxMeanByMonth("2010-02"), err)
    assertEquals(9.79f, maxMeanByMonth("2011-02"), err)
    assertEquals(10.74f, maxMeanByMonth("2013-03"), err)
    assertEquals(13.13f, maxMeanByMonth("2010-03"), err)
    assertEquals(18.55f, maxMeanByMonth("2012-03"), err)
    assertEquals(13.74f, maxMeanByMonth("2011-03"), err)
    assertEquals(9.28f, maxMeanByMonth("2003-02"), err)
    assertEquals(10.41f, maxMeanByMonth("2004-02"), err)
    assertEquals(9.15f, maxMeanByMonth("2005-02"), err)
    assertEquals(8.9f, maxMeanByMonth("2006-02"), err)
    assertEquals(12.34f, maxMeanByMonth("2000-02"), err)
    assertEquals(12.16f, maxMeanByMonth("2001-02"), err)
    assertEquals(11.84f, maxMeanByMonth("2002-02"), err)
  }

  @Test
  def `key events` {
    val q = new LinkedBlockingQueue[String]
    val map = getClientMap[Int, String]()
    // Value mapping doesn't work, and probably would be weird?
    val reg = map.filterKeys(42, 43, 44).mapValues(_.toUpperCase).onEntryEvents() {
      case EntryAdded(key, value) => q offer value
    }
    map.set(42, "Hello")
    val hello = q.poll(5, TimeUnit.SECONDS)
    assertEquals("Hello", hello)
    map.set(999, "World")
    assertNull(q.poll(2, TimeUnit.SECONDS))
    reg.cancel()
  }

  @Test
  def variance {
    val dMap = getClientMap[Int, BigDecimal]()
    1 to 120 foreach { n =>
      dMap.set(n, n)
    }
    val variance = dMap.map(_.value).variance().await.get
    assertEquals(BigDecimal(1210), variance)
    val grouped = dMap.map(_.value).groupBy { n =>
      if (n <= 30) "a"
      else if (n <= 60) "b"
      else if (n <= 90) "c"
      else "d"
    }.variance().await
    assertEquals(4, grouped.size)
    grouped.values.foreach { variance =>
      assertEquals(BigDecimal(77.5d), variance.setScale(1, HALF_EVEN))
    }
  }

  @Test
  def sorting {
    val mymap = getClientMap[String, Int]()
    ('a' to 'z') foreach { c =>
      mymap.set(c.toString, c - 'a')
    }
    val descStrings = mymap.map(_.key).sorted.reverse.take(3).fetch().await
    assertEquals(IndexedSeq("z", "y", "x"), descStrings)
    val descInts = mymap.map(_.value).sorted.reverse.take(3).fetch().await
    assertEquals(IndexedSeq(25, 24, 23), descInts)
  }

}
