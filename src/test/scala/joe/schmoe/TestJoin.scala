package joe.schmoe

import java.util.UUID

import scala.BigDecimal.RoundingMode._
import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.junit.Assert._
import org.junit.Test

import com.hazelcast.Scala._

object TestJoin extends ClusterSetup {
  override val clusterSize = 3
  def init = ()
  def destroy = ()

  case class Id[T](uuid: UUID = UUID.randomUUID)
  type Price = BigDecimal
  type ProdId = Id[Product]
  type CustId = Id[Customer]
  case class OrdId(uuid: UUID = UUID.randomUUID)(custId: CustId)
      extends PartitionKey(custId)
      with Comparable[OrdId] {

    def compareTo(that: OrdId): Int = this.uuid.compareTo(that.uuid)
  }
  case class Product(id: ProdId, name: String, price: Price)
  case class Customer(id: CustId, name: String)
  case class Order(id: OrdId, products: Map[ProdId, Int], customer: CustId)
}

class TestJoin {

  import TestJoin._

  @Test
  def `let's join maps` {
    val customerMap = {
      val map = client.getMap[CustId, Customer]("customers")
      List("Alice", "Bob", "Carl").foreach { name =>
        val c = new Customer(new CustId(), name)
        map.set(c.id, c)
      }
      map
    }
    val productMap = {
      val map = client.getMap[ProdId, Product]("products")
      List("Aged Cheese, 1 kg" -> 35d, "Dark Chocolate, 250 grams" -> 4.5, "Red Wine, 1 liter" -> 9.75).foreach {
        case (name, price) =>
          val p = new Product(new ProdId(), name, BigDecimal(price))
          map.set(p.id, p)
      }
      map
    }
    val bobId = customerMap.filter(where("name") = "Bob").map(_.key).values().await.head
    val orderId = new OrdId()(bobId)
    val orderMap = {
      val map = client.getMap[OrdId, Order]("orders")
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
    val err = 0.00005f
    val avgOrderQty = orderMap.flatMap(_.value.products.map(_._2.toFloat)).mean().await.get
    assertEquals(products.map(_._2.toFloat).sum / products.size, avgOrderQty, err)
    val joinQueriedCustomer = orderMap.query(_.getMap[CustId, Customer]("customers"), where.key in orderId) {
      case (customers, _, order) => customers.get(order.customer)
    }
    assertEquals(customerMap.get(bobId), joinQueriedCustomer(orderId))
    val joinGetAs = orderMap.getAs(_.getMap[CustId, Customer]("customers"), orderId) {
      case (customers, order) => customers.get(order.customer)
    }
    assertEquals(customerMap.get(bobId), joinGetAs.get)
  }

}
