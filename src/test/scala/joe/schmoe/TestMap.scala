package joe.schmoe

import java.util.Date
import java.util.Map.Entry
import java.util.UUID
import java.util.concurrent.{ CountDownLatch, LinkedBlockingQueue, TimeUnit }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{ Failure, Random, Success }

import org.junit.Assert._
import org.junit.Test

import com.hazelcast.Scala._
import com.hazelcast.config.{ InMemoryFormat, MapIndexConfig }
import com.hazelcast.core.IMap
import com.hazelcast.map.AbstractEntryProcessor
import com.hazelcast.query.Predicate

object TestMap extends ClusterSetup {
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
      def apply(entry: Entry[String, Int]) = isFactor37(entry.getValue)
    }
    val entryProcessor = new AbstractEntryProcessor[String, Int] {
      def process(entry: Entry[String, Int]): Object = {
        (entry.value * 2).asInstanceOf[Object]
      }
    }
    val result1a = map.executeOnEntries(entryProcessor, predicate37).asScala.asInstanceOf[collection.mutable.Map[String, Int]]
    val verifyFactor37 = result1a.values.forall(obj => isFactor37(obj.asInstanceOf[Int] / 2))
    assertTrue(verifyFactor37)
    val result1b = map.executeOnEntries(entryProcessor, isFactor37).asScala
    val result2a = map.execute(OnValues(isFactor37)) { entry =>
      entry.value * 2
    }
    val result2b = map.map(_.getValue).collect {
      case value if isFactor37(value) => value * 2
    }.fetch().await.sorted
    val result2c = map.map(_.getValue).filter(isFactor37).collect {
      case value => value * 2
    }.fetch().await.sorted
    val result2d = map.filter(e => isFactor37(e.getValue)).map(_.value * 8).map(_ / 4).fetch().await.sorted
    assertEquals(result1a, result1b)
    assertEquals(result1a, result2a)
    assertEquals(result1a.values.toSeq.sorted, result2b)
    assertEquals(result2b, result2c)
    assertEquals(result2c, result2d)
    val thirtySeven = 37
    val result3a = map.executeOnEntries(entryProcessor, where"this = $thirtySeven")
    assertEquals(37 * 2, result3a.get("key37"))
    val result3b = map.execute(OnValues(_ == 37))(entry => entry.value * 2)
    assertEquals(result3a.get("key37"), result3b("key37"))
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
    assertEquals(None, map.map(_.getValue).minMax.await)
    for (n <- 1L to 500L) {
      map.set(n.toString, n)
    }
    val filtered = map.filter(where("this").between(50, 99)).map(_.getValue)
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
    assertTrue(map.map(_.getValue).mean().await.isEmpty)
    for (n <- 1 to 4) map.set(n.toString, n)
    val intAvg = map.map(_.getValue).mean.await.get
    assertEquals(2, intAvg)
    val dblAvg = map.map(_.value.toDouble).mean.await.get
    assertEquals(2.5, dblAvg, 0.00001)
  }

  @Test
  def distribution {
    val memberMap = getMemberMap[Int, String]()
    val clientMap = getClientMap[Int, String]()
    for (_ <- 1 to 100) {
      distribution(clientMap)
      distribution(memberMap)
      //      clientMap.clear()
    }
  }
  private def distribution(map: IMap[Int, String]) {
    val localMap = new java.util.HashMap[Int, String]
    (1 to 1000).foreach { n =>
      val fizzBuzz = new StringBuilder()
      if (n % 3 == 0) fizzBuzz ++= "Fizz"
      if (n % 5 == 0) fizzBuzz ++= "Buzz"
      if (fizzBuzz.isEmpty) fizzBuzz ++= n.toString
      localMap.put(n, fizzBuzz.result)
    }
    map.putAll(localMap)
    val (distribution, distrTime) = timed { map.filterKeys(_ <= 100).map(_.getValue).distribution().await }
    val (distinct, distcTime) = timed { map.filter(where.key <= 100).map(_.getValue).distinct().await }
    assertEquals(distribution.keySet, distinct)
    //    println(s"Distribution: $distrTime ms, Distinct: $distcTime ms, using ${map.getClass.getSimpleName}")
    assertEquals(27, distribution("Fizz"))
    assertEquals(14, distribution("Buzz"))
    assertEquals(6, distribution("FizzBuzz"))
    assertTrue(distribution.filterKeys(!Set("Fizz", "Buzz", "FizzBuzz").contains(_)).forall(_._2 == 1))
    assertEquals(Map(27 -> Set("Fizz")), map.filterKeys(_ <= 100).map(_.getValue).frequency(1).await)
    assertEquals(Map(27 -> Set("Fizz"), 14 -> Set("Buzz"), 6 -> Set("FizzBuzz")), map.filterKeys(_ <= 100).map(_.getValue).frequency(3).await)
  }

  @Test
  def `large map test` {
    val Thousands = 750
    val clientMap = getClientMap[UUID, Employee]("employees")
    var allSalaries = 0d
    var empCount = 0
    (1 to Thousands).foreach { _ =>
      val localMap = new java.util.HashMap[UUID, Employee]
      (1 to 1000).foreach { _ =>
        val id = UUID.randomUUID()
        val salary = Random.nextInt(480000) + 20000
        val emp = new Employee(id, randomString(), salary)
        localMap.put(id, emp)
        allSalaries += salary
        empCount += 1
      }
      clientMap.putAll(localMap)
    }
    assertEquals(Thousands * 1000, empCount)
    val localAvg = (allSalaries / empCount).toInt
    val (avg, ms) = timed {
      clientMap.map(_.getValue).map(_.salary.toDouble).mean.await.get.toInt
    }
    println(s"Average salary : $$$avg ($ms ms)")
    assertEquals(localAvg, avg)
    val (fCount, fCountMs) = timed(clientMap.filterValues(_.salary > 495000).count.await)
    val (cCount, cCountMs) = timed(clientMap.filter(where("salary") > 495000).count.await)
    println(s"Filter: $fCount employees make more than $$495K ($fCountMs ms)")
    println(s"Predicate: $cCount employees make more than $$495K ($cCountMs ms)")
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
    val strValues = strMap.map(_.getValue)
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
    val intValues = intMap.map(_.getValue)
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
    val values = map.map(_.getValue)
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
    println(s"Values $freqValues all occurred $freq times")
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
    val words = aliceChapters.map(_.getValue).flatMap { chapter =>
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
    val maxByMonthYear = milanWeather.map { entry =>
      val date = entry.key
      val yearMonth = (date.getYear + 1900) -> (date.getMonth + 1)
      yearMonth -> entry.value.tempMax
    }.groupBy(_._1, _._2)
    val (result, ms) = timed { maxByMonthYear.mean().await }
    println(s"Milan monthly mean max temp: $ms ms")
    val err = 0.005f
    assertEquals(7.23f, result(2012 -> 2), err)
    assertEquals(7.2f, result(2013 -> 2), err)
    assertEquals(7.85f, result(2010 -> 2), err)
    assertEquals(9.79f, result(2011 -> 2), err)
    assertEquals(10.74f, result(2013 -> 3), err)
    assertEquals(13.13f, result(2010 -> 3), err)
    assertEquals(18.55f, result(2012 -> 3), err)
    assertEquals(13.74f, result(2011 -> 3), err)
    assertEquals(9.28f, result(2003 -> 2), err)
    assertEquals(10.41f, result(2004 -> 2), err)
    assertEquals(9.15f, result(2005 -> 2), err)
    assertEquals(8.9f, result(2006 -> 2), err)
    //    assertEquals(12.34f, result(2000 -> 2), err)
    //    assertEquals(12.16f, result(2001 -> 2), err)
    assertEquals(11.84f, result(2002 -> 2), err)
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

}
