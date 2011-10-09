/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core.dispatcher

import scala.collection.mutable.HashSet
import akka.dispatch.MessageInvocation
import scala.collection.mutable.ListBuffer
import akka.setack.core.TestMessageInvocationSequence

/**
 * The schedule is a set of partial orders between the
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 * test message invocations (test message invocation sequence).
 */
class TestSchedule {

  private var testMessageInvocationSequences = new HashSet[TestMessageInvocationSequence]

  def this(schedule: Set[TestMessageInvocationSequence]) {
    this()
    for (testMessageInvocationSequence ← schedule)
      testMessageInvocationSequences.+=(testMessageInvocationSequence)
  }

  /**
   * @return The least index of the message among all the sequences.
   * There are multiple cases for the returned index:
   * 1) the index is zero: the message can be delivered
   * 2) the index is greater than zero: the message should be delivered later
   * 3) the index is -1: the message can be delivered without any constraints
   * (the message is not matched with any messages in the partial orders)
   */
  def leastIndexOf(messageInvocation: MessageInvocation): Int = synchronized {
    var leastIndex = -1
    for (testMessageInvocationSequence ← testMessageInvocationSequences) {
      val currIndex = testMessageInvocationSequence.indexOf(messageInvocation)
      if (currIndex == 0) return currIndex
      else if (currIndex > 0 && leastIndex == -1)
        leastIndex = currIndex
      else if (currIndex > 0 && leastIndex > -1)
        leastIndex = math.min(currIndex, leastIndex)
    }
    return leastIndex

  }
  /**
   * It is called by the dispatcher to remove the message that is in the head
   * of partial orders in the schedule (move forward the pointer for the current
   * schedule).
   */
  def removeFromHead(messageInvocation: MessageInvocation): Boolean = synchronized {
    for (testMessageInvocationSequence ← testMessageInvocationSequences) {
      if (testMessageInvocationSequence.head != null &&
        testMessageInvocationSequence.head.matchWithRealInvocation(messageInvocation)) {
        val result = testMessageInvocationSequence.removeHead
        if (testMessageInvocationSequence.isEmpty)
          testMessageInvocationSequences.remove(testMessageInvocationSequence)
        return result
      }
    }
    return false

  }

  def isEmpty = synchronized {
    testMessageInvocationSequences.isEmpty
  }

  override def toString(): String = synchronized {
    var outString = "schedule = "
    for (sequnece ← testMessageInvocationSequences) {
      outString += sequnece.messageSequence.mkString("->")
      outString += ", "
    }
    return outString
  }

}