/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.util
import akka.setack.core.monitor.Monitor
import scala.collection.mutable.HashSet
import akka.setack.core.TestExecutionManager

/**
 * This object contains different kinds of assertions that can be used by the
 * user in the test.
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
object Assert {

  /**
   * Waits for the system to get stable and then checks the expression. Throws exception
   * if the system does not get stable after maximum try.
   */
  def assertWhenStable(expression: ⇒ Boolean, message: String = "") {
    val isStable = TestExecutionManager.checkForStability()
    if (isStable) {
      assert(expression, message)
    } else {
      throw new Exception("the system did not get stable with the specified timeout")
    }
  }

  /**
   * Checks the expression after some timeout defined by the user.
   * Useful when the system may not get stable but still it is safe to check
   * the assertion.
   */
  def assertAfter(millis: Long, expression: ⇒ Boolean, message: String = "") {
    Thread.sleep(millis)
    assert(expression, message)
  }

}