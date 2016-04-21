package joe.schmoe

import com.hazelcast.Scala._
import org.junit._, Assert._
import javax.script.ScriptEngineManager
import javax.script.ScriptContext

object TestDynamicExecution extends ClusterSetup {
  override val clusterSize = 3

  def init {
    serialization.DynamicExecution.register(memberConfig.getSerializationConfig)
  }
  def destroy = ()

  val ScriptEngine = {
    val engine = new scala.tools.nsc.interpreter.IMain
//    val engine = new ScriptEngineManager().getEngineByName("scala")
    val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    settings.embeddedDefaults[TestDynamicExecution.type]
    engine
  }

}

class TestDynamicExecution {
  import TestDynamicExecution._

  @Test @Ignore
  def `updating/upserting` {
    val theMap = getClientMap[String, Int]()
    ScriptEngine.bind("theMap", theMap)
    ScriptEngine.getContext.setAttribute("theMap", theMap, ScriptContext.ENGINE_SCOPE)
    val bindings = ScriptEngine.createBindings()
    bindings.put("theMap", theMap)
    ScriptEngine.eval("""theMap.updateAndGet("foo")(_ + 1)""", bindings) match {
      case None => // Ok
      case Some(result) => fail(s"Should not have updated empty map, result = $result")
    }
    ScriptEngine.eval("""theMap.upsertAndGet("foo", 42)(_ + 1)""", bindings) match {
      case n: Number => assertEquals(42, n)
    }
    ScriptEngine.eval("""theMap.upsertAndGet("foo", 99)(_ + 13)""", bindings) match {
      case n: Number => assertEquals(55, n)
    }
    theMap.put("bar", 44)
    ScriptEngine.eval("""theMap.map(_.value).map(_.toFloat).mean().await""", bindings) match {
      case n: Number => assertEquals((55 + 44) / 2f, n)
    }
  }
}
