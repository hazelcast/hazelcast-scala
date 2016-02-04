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
import java.util.StringTokenizer

object TestJoin extends ClusterSetup {
  override val clusterSize = 3
  def init {
    TestSerializers.register(clientConfig.getSerializationConfig)
    TestSerializers.register(memberConfig.getSerializationConfig)
    memberConfig.getSerializationConfig.setAllowUnsafe(true)
    clientConfig.getSerializationConfig.setAllowUnsafe(true)
  }
  def destroy = ()

  case class Id[T](uuid: UUID = UUID.randomUUID)
  type Price = BigDecimal
  type ProdId = Id[Product]
  type CustId = Id[Customer]
  type OrdId = Id[Order]
  case class Product(id: ProdId, name: String, price: Price)
  case class Customer(id: CustId, name: String)
  case class Order(id: OrdId, products: Map[ProdId, Int], customer: CustId)

}

class TestJoin {

  import TestJoin._

  @Test
  def `let's join maps` {
    val customerMap = {
      val map = getClientMap[CustId, Customer]("customers")
      List("Alice", "Bob", "Carl").foreach { name =>
        val c = new Customer(new CustId(), name)
        map.set(c.id, c)
      }
      map
    }
    val productMap = {
      val map = getClientMap[ProdId, Product]("products")
      List("Aged Cheese, 1 kg" -> 35d, "Dark Chocolate, 250 grams" -> 4.5, "Red Wine, 1 liter" -> 9.75).foreach {
        case (name, price) =>
          val p = new Product(new ProdId(), name, BigDecimal(price))
          map.set(p.id, p)
      }
      map
    }
    val orderId = new OrdId()
    val orderMap = {
      val map = getClientMap[OrdId, Order]("orders")
      val bobId = customerMap.filter(where("name") = "Bob").map(_.key).values().await.head
      val productQtys = productMap.keySet().asScala.zipWithIndex.map {
        case (productId, idx) => productId -> (idx + 1) * 3
      }.toMap
      val order = Order(orderId, productQtys, bobId)
      map.set(order.id, order)
      map
    }
    val (order, customer, products) =
      orderMap.filterKeys(orderId)
        .map(_.value)
        .innerJoinOne(customerMap, _.customer)
        .innerJoinMany(productMap, _._1.products.keySet).collect {
          case ((order, customer), products) =>
            val prodQty = order.products.toSeq.map {
              case (prodId, qty) => products(prodId) -> qty
            }
            (order, customer, prodQty)
        }.values.await.head
    assertEquals(order.customer, customer.id)
    val avgOrderQty = orderMap.flatMap(_.value.products.map(_._2)).mean().await.get
    assertEquals(products.map(_._2).sum / products.size, avgOrderQty)
  }

}
