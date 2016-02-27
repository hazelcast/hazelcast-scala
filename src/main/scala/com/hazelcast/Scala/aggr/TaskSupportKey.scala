package com.hazelcast.Scala.aggr

import scala.collection.parallel.TaskSupport
import com.hazelcast.Scala.UserContext

/**
 *  User context key for overriding the
 *  default TastSupport for parallel collections.
 *
 *  Example:
 *  ```scala
 *  hz.userCtx(TaskSupportKey) = new ForkJoinTaskSupport()
 *  ```
 */
object TaskSupportKey extends UserContext.Key[TaskSupport]
