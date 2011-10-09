/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.util
import akka.setack.core.TestExecutionManager
import akka.setack.core.TestMessageInvocationSequence
import akka.setack.core.dispatcher.TestDispatcher
import akka.setack.core.dispatcher.TestSchedule

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

object TestExecutionUtil {

  def whenStable(body: ⇒ Unit)(implicit tryCount: Int = 10): Boolean = {
    val isStable = TestExecutionManager.checkForStability(tryCount)
    body

    if (isStable) {
      return true
    } else {
      return false
    }

  }

  /**
   * API for setting the schedule of test execution and constraining the
   * order of test messages.
   */
  def setSchedule(partialOrders: TestMessageInvocationSequence*) {
    var pordersSet = partialOrders.toSet[TestMessageInvocationSequence]
    TestDispatcher.currentSchedule_=(new TestSchedule(pordersSet))
  }

}