package joe.schmoe

import java.util.UUID
import scala.util.Random

case class Employee(id: UUID, name: String, salary: Int, age: Int, active: Boolean)

object Employee {
  def random: Employee = {
    val name = randomString(20)
    val salary = Random.nextInt(480000) + 20000
    val age = Random.nextInt(60) + 20
    val active = Random.nextInt(20) == 0
    new Employee(UUID.randomUUID, name, salary, age, active)
  }
}
