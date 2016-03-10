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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.util._

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
    memberConfig.getMapConfig("employees").
      addMapIndexConfig(new MapIndexConfig("salary", true)).
      setInMemoryFormat(InMemoryFormat.OBJECT)
    memberConfig.getSerializationConfig.setAllowUnsafe(true)
    clientConfig.getSerializationConfig.setAllowUnsafe(true)
  }
  def destroy = ()

  case class MyNumber(num: Int)

}

class TestMap {

  import TestMap._

  @Test
  def upsert {
    val map = getClientMap[UUID, Int]()
    DeltaUpdateTesting.testUpsert(map, map.get)
  }

  @Test
  def asyncPutIfAbsent {
    val map = getClientMap[Int, String]()
    val firstPut = map.async.putIfAbsent(5, "Hello").await
    assertEquals(None, firstPut)
    val secondPut = map.async.putIfAbsent(5, "World").await
    assertEquals(Some("Hello"), secondPut)
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
  def update {
    val map = getClientMap[UUID, Int]()
    DeltaUpdateTesting.testUpdate(map, map.get, map.put(_, _))
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
    val tempMap = new java.util.HashMap[String, Int](count)
    for (i <- 1 to count) {
      tempMap.put("key" + i, i)
    }
    map.putAll(tempMap)
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
    assertTrue(verifyFactor37)
    val result1b = map.executeOnEntries(entryProcessor, isFactor37).asScala
    val result2a = map.execute(OnValues(isFactor37)) { entry =>
      entry.value * 2
    }
    val result2b = map.map(_.value).collect {
      case value if isFactor37(value) => value * 2
    }.values().await.sorted
    val result2c = map.map(_.value).filter(isFactor37).collect {
      case value => value * 2
    }.values().await.sorted
    val result2d = map.filter(e => isFactor37(e.value)).map(_.value * 8).map(_ / 4).values().await.sorted
    assertEquals(result1a, result1b)
    assertEquals(result1a, result2a)
    assertEquals(result1a.values.toSeq.sorted, result2b)
    assertEquals(result2b, result2c)
    assertEquals(result2c, result2d)
    val thirtySeven = 37
    val result3a = map.executeOnEntries(entryProcessor, where.value = thirtySeven)
    assertEquals(37 * 2, result3a.get("key37"))
    val result3b = map.execute(OnValues(_ == 37))(entry => entry.value * 2)
    val result3c = map.query(where.value = 37)(_ * 2)
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
        val (distribution, distrTime) = timed(unit = MICROSECONDS) { map.filterKeys(_ <= 100).map(_.value).distribution().await }
        val (distinct, distcTime) = timed(unit = MICROSECONDS) { map.filter(where.key <= 100).map(_.value).distinct().await }
        assertEquals(distribution.keySet, distinct)
        assertEquals(distribution.keySet, distinct)
        if (printTimings) println(s"Distribution: $distrTime μs, Distinct: $distcTime μs, using ${map.getClass.getSimpleName}")
        assertEquals(27, distribution("Fizz"))
        assertEquals(14, distribution("Buzz"))
        assertEquals(6, distribution("FizzBuzz"))
        assertTrue(distribution.filterKeys(!Set("Fizz", "Buzz", "FizzBuzz").contains(_)).forall(_._2 == 1))
        val top1 = map.filterKeys(_ <= 100).map(_.value).distribution().map { dist =>
          dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(1).next
        }.await
        assertEquals(27 -> Set("Fizz"), top1)
        val top3 = map.filter(where.key <= 100).map(_.value).distribution().map { dist =>
          dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(3).toMap
        }.await
        assertEquals(Map(27 -> Set("Fizz"), 14 -> Set("Buzz"), 6 -> Set("FizzBuzz")), top3)
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
    val firstByKey = map.values(where.key() = sqlPredFirst.id) // Generally, you wouldn't do this, since there's `get`
    assertEquals(sqlPredFirst, firstByKey.asScala.head)
    val sqlSalaries = sqlResult.asScala.map(e => e.id -> e.salary).toMap
    val querySalaryResult = map.query(sqlPred)(_.salary)
    assertEquals(sqlSalaries, querySalaryResult)
  }

  @Test
  def `large map test` {
    val OneThousand = 1000
    val Thousands = 250
    val clientMap = getClientMap[UUID, Employee]("employees")
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
    assertEquals(Thousands * OneThousand, empCount)

    val (inlineStats, inlineStatsMs) = timed(warmups = 3) {
      clientMap.aggregate(new Stats())({
        case (stats, entry) => stats.update(entry.value.salary)
      }, {
        case (x, y) => x.mergeWith(y)
      }).await
    }
    assertEquals(empCount, inlineStats.count)
    assertEquals(allSalaries, inlineStats.sum)
    assertEquals(minSal, inlineStats.min)
    assertEquals(maxSal, inlineStats.max)
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
    assertEquals(localAvg, inlineAvg)
    val (avg, avgMs) = timed(warmups = 3) {
      clientMap.map(_.value.salary.toLong).mean.await.get
    }
    assertEquals(localAvg, avg)
    println(s"Average salary : $$$avg")
    val (mrAvg, mrMs) = timed(warmups = 1) {
      import com.hazelcast.mapreduce.aggregation._
      val supplySalaryForAll = new Supplier[UUID, Employee, Long] {
        def apply(entry: Entry[UUID, Employee]): Long = entry.value.salary.toLong
      }
      val averageAggr = Aggregations.longAvg.asInstanceOf[Aggregation[UUID, Long, Long]]
      clientMap.aggregate(supplySalaryForAll, averageAggr)
    }
    assertEquals(avg, mrAvg)
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
    assertEquals(20, javaPage.size)
    assertEquals(javaPage.size, scalaPage.size)
    val javaSal = javaPage.map(_.salary).toIndexedSeq
    val scalaSal = scalaPage.map(_.salary)
    println(s"First salary: $$${scalaSal(0)} (Scala), $$${javaSal(0)} (Java)")
    (0 until javaSal.size).foreach { idx =>
      assertEquals(idx -> javaSal(idx), idx -> scalaSal(idx))
    }

    clientMap.clear()
    clientMap.destroy()
  }

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
    val (freq, freqValues) = values.distribution().map { dist =>
      dist.groupBy(_._2).mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(1).next
    }.await
    assertEquals(localModeValues, valueSet)
    assertTrue(freqValues.nonEmpty)
    assertEquals(localModeFreq, freq)
    assertEquals(localModeValues, freqValues)
    println(s"Value(s) `${freqValues.mkString(",")}` occurred $freq times")
    freqValues.foreach { value =>
      assertEquals(freq, dist(value))
    }
  }

  @Test
  def `wordcount: alice in wonderland` {
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
    val (top10ByCount, countMs) = timed() {
      val countByWord = words.groupBy().count().await
      val foo = countByWord.groupBy(_._2)
      foo.mapValues(_.keySet).toSeq.sortBy(_._1).reverseIterator.take(10).toMap
    }
    assertEquals(expectedTopTen, top10ByCount)
    println(s"Top 10 words took $countMs ms")

    val longestWord = words.max(_.length).await.get
    println(s""""Alice in Wonderland", longest word: $longestWord""")
    val top5LongestWords = words.sortBy(_.length).reverse.take(5).values.await
    assertTrue(top5LongestWords contains longestWord)
    val topStr = top5LongestWords.map(w => s"$w (${w.length})").mkString(", ")
    println(s""""Alice in Wonderland", top 5 longest words: $topStr""")
  }

  @Test
  def `wordcount: flatland` {
    val flatlandChapters = getClientMap[String, String]("flatland")
    val flatlandSrc = Source.fromInputStream(getClass.getResourceAsStream("/flatland.txt"), "UTF-8")
    val sb = new java.lang.StringBuilder
    var currChapter: String = "PREFACE"
      def saveChapter() {
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
    assertEquals(20, topTwenty.size)
    // http://www.slideshare.net/andreaiacono/mapreduce-34478449/24
    assertEquals(2286, topTwenty("the"))
    assertEquals(1634, topTwenty("of"))
    assertEquals(1098, topTwenty("and"))
    assertEquals(1088, topTwenty("to"))
    assertEquals(936, topTwenty("a"))
    assertEquals(735, topTwenty("i"))
    assertEquals(713, topTwenty("in"))
    assertEquals(499, topTwenty("that"))
    assertEquals(429, topTwenty("is"))
    assertEquals(419, topTwenty("you"))
    assertEquals(334, topTwenty("my"))
    assertEquals(330, topTwenty("it"))
    assertEquals(322, topTwenty("as"))
    assertEquals(317, topTwenty("by"))
    assertEquals(317, topTwenty("not"))
    assertEquals(299, topTwenty("or"))
    assertEquals(279, topTwenty("but"))
    assertEquals(273, topTwenty("with"))
    assertEquals(267, topTwenty("for"))
    assertEquals(252, topTwenty("be"))

    val longestWord = words.max(_.length).await.get
    println(s""""Flatland", longest word: $longestWord""")
    val top5LongestWords = words.sortBy(_.length).reverse.take(5).values.await
    assertTrue(top5LongestWords contains longestWord)
    val topStr = top5LongestWords.map(w => s"$w (${w.length})").mkString(", ")
    println(s""""Flatland", top 5 longest words: $topStr""")

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
      val yearMonthFmt = new java.text.SimpleDateFormat("MMyyyy")
      val date = entry.key
      val yearMonth = yearMonthFmt.format(date)
      yearMonth -> entry.value.tempMax
    }
    val byMonthYear = monthYearMax.groupBy(_._1, _._2)
    val maxMeanByMonth = byMonthYear.mean.await
    val err = 0.005f

    // http://www.slideshare.net/andreaiacono/mapreduce-34478449/39
    assertEquals(7.23f, maxMeanByMonth("022012"), err)
    assertEquals(7.2f, maxMeanByMonth("022013"), err)
    assertEquals(7.85f, maxMeanByMonth("022010"), err)
    assertEquals(9.79f, maxMeanByMonth("022011"), err)
    assertEquals(10.74f, maxMeanByMonth("032013"), err)
    assertEquals(13.13f, maxMeanByMonth("032010"), err)
    assertEquals(18.55f, maxMeanByMonth("032012"), err)
    assertEquals(13.74f, maxMeanByMonth("032011"), err)
    assertEquals(9.28f, maxMeanByMonth("022003"), err)
    assertEquals(10.41f, maxMeanByMonth("022004"), err)
    assertEquals(9.15f, maxMeanByMonth("022005"), err)
    assertEquals(8.9f, maxMeanByMonth("022006"), err)
    assertEquals(12.34f, maxMeanByMonth("022000"), err)
    assertEquals(12.16f, maxMeanByMonth("022001"), err)
    assertEquals(11.84f, maxMeanByMonth("022002"), err)

    val topTen = maxMeanByMonth.toSeq.sortBy(_._2).reverseIterator.take(10).toIndexedSeq.map {
      case (my, temp) => my -> (math.round(temp * 100) / 100f)
    }
    assertEquals("082003" -> 35.29f, topTen(0))
    assertEquals("062003" -> 33.04f, topTen(1))
    assertEquals("072006" -> 32.71f, topTen(2))
    assertEquals("082001" -> 32.38f, topTen(3))
    assertEquals("072003" -> 32.04f, topTen(4))
    assertEquals("072007" -> 31.91f, topTen(5))
    assertEquals("082012" -> 31.81f, topTen(6))
    assertEquals("082009" -> 31.68f, topTen(7))
    assertEquals("072004" -> 31.27f, topTen(8))
    assertEquals("072005" -> 31.25f, topTen(9))
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
    val bdMap = getClientMap[Int, BigDecimal]()
    1 to 120 foreach { n =>
      bdMap.set(n, n)
    }
    val variance = bdMap.map(_.value).variance(_ - 1).await.get
    assertEquals(BigDecimal(1210), variance)
    val dVar = bdMap.map(_.value.doubleValue).variance(_ - 1).await.get
    assertEquals(1210d, dVar, 0.0001)
    val grouped = bdMap.map(_.value).groupBy { n =>
      if (n <= 30) "a"
      else if (n <= 60) "b"
      else if (n <= 90) "c"
      else "d"
    }.variance(_ - 1).await
    assertEquals(4, grouped.size)
    grouped.values.foreach { variance =>
      assertEquals(BigDecimal(77.5d), variance.setScale(1, HALF_EVEN))
    }

    val dMap = getClientMap[String, Double]()
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
    assertEquals(5d, meanDbl, err)
    assertEquals(BigDecimal("5"), meanBD)
    val stdDev = dMap.filterKeys(keys).map(_.value).variance().await.map(math.sqrt).get
    assertEquals(2d, stdDev, err)

    dMap.set("foo", 5d)
    val varianceSingleEntry = dMap.filterKeys("foo").map(_.value).variance(_ - 1).await.get
    assertEquals(0d, varianceSingleEntry, err)
    val varianceNoEntry = dMap.filterKeys("bar").map(_.value).variance().await
    assertEquals(None, varianceNoEntry)

    val localValues = (1 to 10000).map(_ => Random.nextInt(90) + (Random.nextDouble + 10d)).map(BigDecimal(_))
    val m = localValues.sum / localValues.size
    val localVariance = localValues.map(n => (n - m) * (n - m)).sum / localValues.size
    val numMap = localValues.foldLeft(getClientMap[UUID, BigDecimal]()) {
      case (map, bd) => map.set(UUID.randomUUID, bd); map
    }
    val hzVarianceDbl = numMap.map(_.value.toDouble).variance().await.get
    val hzVarianceBD = numMap.map(_.value).variance().await.get
    assertEquals(localVariance, hzVarianceBD)
    assertEquals(localVariance.toDouble, hzVarianceDbl, 0.00000001)
  }

  @Test
  def `sorted dds` {
    val mymap = getClientMap[String, Int]()
    ('a' to 'z') foreach { c =>
      mymap.set(c.toString, c - 'a')
    }
    val pageSize = 3
    val descStrings = mymap.map(_.key.toUpperCase).sorted.reverse
      def skipPages(p: Int) = p * pageSize
    val descStringsP1 = descStrings.take(pageSize).values().await
    val descStringsP2 = descStrings.drop(skipPages(1)).take(pageSize).values().await
    assertEquals(IndexedSeq("Z", "Y", "X"), descStringsP1)
    assertEquals(IndexedSeq("W", "V", "U"), descStringsP2)
    val descInts = mymap.map(_.value).sorted.reverse
    val descIntsP1 = descInts.drop(skipPages(0)).take(pageSize).values().await
    assertEquals(IndexedSeq(25, 24, 23), descIntsP1)
    val descIntsMean = descInts.take(10).mean().await.get
    assertEquals((25 + 24 + 23 + 22 + 21 + 20 + 19 + 18 + 17 + 16) / 10, descIntsMean)
  }

  @Test
  def paging {
    import java.util.Comparator
    import com.hazelcast.query.PagingPredicate

    val employees = (1 to 1000).foldLeft(getClientMap[UUID, Employee]()) {
      case (employees, _) =>
        val emp = Employee.random
        employees.set(emp.id, emp)
        employees
    }
    val wellPaid = where("salary") >= 400000
    type E = Entry[UUID, Employee]
    val asc = new Comparator[E] with Serializable {
      def compare(a: E, b: E): Int = a.value.salary.compareTo(b.value.salary)
    }.asInstanceOf[Comparator[Entry[_, _]]]
    val desc = java.util.Collections.reverseOrder(asc)
    val ppAsc = new PagingPredicate(wellPaid, asc, 10)
    ppAsc.setPage(4)
    val fromJavaAsc = employees.values(ppAsc).asScala.map(_.salary)
    assertEquals(10, fromJavaAsc.size)
    val ppDesc = new PagingPredicate(wellPaid, desc, 10)
    ppDesc.setPage(3)
    val fromJavaDesc = employees.values(ppDesc).asScala.map(_.salary)
    assertEquals(10, fromJavaDesc.size)
    val fromScalaAsc = employees.values(40 until 50, wellPaid)(_.value.salary).map(_.salary)
    assertEquals(10, fromScalaAsc.size)
    val fromScalaDesc = employees.values(30 until 40, wellPaid)(_.value.salary, reverse = true).map(_.salary)
    assertEquals(10, fromScalaDesc.size)
    assertEquals(fromJavaAsc, fromScalaAsc)
    assertEquals(fromJavaDesc, fromScalaDesc)
  }

  @Test
  def stringLengthMinMax1() {
    val strMap = getClientMap[Int, String]()
    strMap.set(1, "abc")
    strMap.set(2, "abc")
    strMap.set(3, "abcdefghijklmnop")
    strMap.set(4, "abcdef")
    strMap.set(5, "abcxyz")
    strMap.set(6, "xyz")

    val maxByStr = strMap.map(_.value).max().await.get
    assertEquals("xyz", maxByStr)
    val maxByLen = strMap.map(_.value).max(_.length).await.get
    assertEquals("abcdefghijklmnop", maxByLen)

    val longestByMax = strMap.max(_.value.length).await.get
    assertEquals(3, longestByMax.key)
    assertEquals(strMap.get(3), longestByMax.value)
    val longestBySort = strMap.sortBy(_.value.length).reverse.take(1).values.await.head
    assertEquals(3, longestBySort.key)
    assertEquals(strMap.get(3), longestBySort.value)
  }
  @Test
  def stringLengthMinMax2() {
    val localMap = (1 to 250000).foldLeft(new java.util.HashMap[Int, String]) {
      case (map, key) =>
        map.put(key, randomString(500))
        map
    }
    val strMap = getClientMap[Int, String]()
    strMap.putAll(localMap)
    for (i <- 1 to 5) {
      val (bySort, bySortTime) = timed()(strMap.sortBy(_.value.length).reverse.take(1).values.await.head)
      val (byMax, byMaxTime) = timed()(strMap.max(_.value.length).await.get)
      assertEquals(byMax.key, bySort.key)
      if (i >= 3) println(s"Longest string: max(): $byMaxTime ms, sortBy(): $bySortTime ms")
    }
    strMap.clear()
    strMap.destroy()
  }

  @Test
  def aggregateToMap() {
    var localSum = 0L
    var localCount = 0
    val employees = (1 to 50000).foldLeft(getClientMap[UUID, Employee]()) {
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
    assertEquals(localSum, remoteSum)
    assertEquals(localCount, remoteCount)

    val resultMap = getClientMap[String, (Long, Int)]()
    val resultKey = "salarySumCount"
    resultMap.delete(resultKey)
    employees.map(_.value).aggregateInto(resultMap, resultKey)(0L -> 0)({
      case ((sum, count), emp) => (sum + emp.salary) -> (count + 1)
    }, {
      case (x, y) => (x._1 + y._1) -> (x._2 + y._2)
    }).await
    val (sum, count) = resultMap.get(resultKey)
    assertEquals(localSum, sum)
    assertEquals(localCount, count)

    val resultsByAge = getClientMap[Int, (Long, Int)]()
    val ages = employees.map(_.value).groupBy(_.age).aggregateInto(resultsByAge)(0L -> 0)({
      case ((sum, count), emp) => (sum + emp.salary) -> (count + 1)
    }, {
      case (x, y) => (x._1 + y._1) -> (x._2 + y._2)
    }).await
    assertEquals(ages.size, resultsByAge.size)
    val (ageSum, ageCount) = ages.foldLeft(0L -> 0) {
      case ((totalSum, totalCount), age) =>
        val (sum, count) = resultsByAge.get(age)
        assertTrue(count > 0)
        (totalSum + sum) -> (totalCount + count)
    }
    assertEquals(localSum, ageSum)
    assertEquals(localCount, ageCount)

    employees.clear()
    employees.destroy()
  }
}
