/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.util
import akka.setack.core.TestExecutionManager
import akka.setack.core.TestMessageInvocationSequence
import akka.setack.core.TestSchedule
import akka.setack.core.TestActorRef
import akka.actor.Actor

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

object TestExecutionUtil {

  def whenStable(body: ⇒ Unit)(implicit tryCount: Int = 10): Boolean = {
    val isStable = TestExecutionManager.checkForStability(tryCount)
    body
    isStable

  }

  /**
   * API for constraining the schedule of test execution and removing some non-determinism by specifying
   * a set of partial orders between the messages. The receivers of the messages in each partial order should
   * be the same (an instance of TestActorRef)
   */
  def setSchedule(partialOrders: TestMessageInvocationSequence*) {
    var pordersSet = partialOrders.toSet[TestMessageInvocationSequence]
    /*
     * TODO: check if the receivers of all messages in each partial order are the same
     */
    for (po ← partialOrders) {
      po.head._receiver.asInstanceOf[TestActorRef].addPartialOrderToSchedule(po)
    }
  }

}