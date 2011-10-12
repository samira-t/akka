/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core.dispatcher
import akka.dispatch.Dispatcher
import akka.dispatch.MessageInvocation
import akka.actor.ActorRef
import scala.collection.mutable.HashSet
import akka.actor.ActorInitializationException
import akka.actor.Actor
import akka.setack.core.monitor.AsyncMessageEvent
import akka.setack.core.MessageEventEnum._
import akka.setack.core.monitor.Monitor
import akka.dispatch.TaskInvocation
import akka.setack.core.TestActorRef

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

class TestDispatcher extends Dispatcher("TestDispatcher") {

  /**
   * A holder for the messages that are supposed to be delivered later
   */
  var cloudMessages = new HashSet[MessageInvocation]()

  /**
   * The current schedule of the test execution that needs to be followed
   */
  private var _currentSchedule: TestSchedule = null

  /**
   * Overrides the dispatch  method from  ExecutorBasedEventDrivenDispatcher in order to:
   * 1) check if the delivery of the message can be done according to the schedule
   * 2) inform the monitor actor about the delivery
   */
  override protected[akka] def dispatch(invocation: MessageInvocation) = {
    //println("dispatch is called" + invocation.message)
    getMailbox(invocation.receiver) match {
      case null ⇒ throw new ActorInitializationException("Actor has not been started, you need to invoke 'actor.start()' before using it")
      case mbox ⇒
        if (_currentSchedule != null) {
          _currentSchedule.leastIndexOf(invocation) match {
            case -1 ⇒ dispatchWithoutCheck(invocation, false)
            case 0  ⇒ dispatchWithoutCheck(invocation, true)
            case _  ⇒ cloudMessages.add(invocation) //; println("added to cloud" + invocation)
          }
        } else dispatchWithoutCheck(invocation, false)
    }
  }

  /**
   * Performs the delivery without checking with the schedule
   * it will update the schedule if the message that is going to delivered is in the
   * head of any partial orders in the schedule. It is determined by the dispatch method.
   */
  private def dispatchWithoutCheck(invocation: MessageInvocation, updateSchedule: Boolean) = {
    getMailbox(invocation.receiver) match {
      case null ⇒ throw new ActorInitializationException("Actor has not been started, you need to invoke 'actor.start()' before using it")
      case mbox ⇒
        mbox enqueue invocation
        if (updateSchedule) {
          removeFromSchedule(invocation)
          //println("schedule is updated", _currentSchedule.toString)
        }
        Monitor.traceMonitorActor ! AsyncMessageEvent(invocation, Delivered)
        registerForExecution(mbox)
    }
  }

  /**
   * Removes the delivered message from the schedule and checks for the further
   * delivery from the messages in the cloud
   */
  private def removeFromSchedule(invocation: MessageInvocation) {
    assert(_currentSchedule.removeFromHead(invocation))
    checkForDeliveryFromCloud()
    //println("message delivered" + invocation)
  }

  /**
   * Checks if there is any message in the cloud that can be delivered.
   * It is called after the current state of the schedule is updated.
   */
  private def checkForDeliveryFromCloud() {
    var deliveredInvocations = new HashSet[MessageInvocation]
    for (invocation ← cloudMessages) {
      if (_currentSchedule.leastIndexOf(invocation) == 0) {
        dispatchWithoutCheck(invocation, true)
        deliveredInvocations.add(invocation)
      }
    }

    for (invocation ← deliveredInvocations) {
      cloudMessages.-=(invocation)
    }

  }

  def clearState() {
    _currentSchedule = null
    cloudMessages.clear()
  }

  /**
   * The schedule is finished if the current is schedule is empty and
   * all the messages in the schedule are delivered in the order
   */
  def isFinished = ((_currentSchedule == null) || (_currentSchedule.isEmpty && cloudMessages.isEmpty))

  /**
   * Sets the current schedule of the scheduler
   * This is called by the user through the Test object
   */
  def currentSchedule_=(schedule: TestSchedule): Unit = {
    _currentSchedule = schedule
    //println("current schedule= " + _currentSchedule.toString())
  }

}

object TestDispatcher {
  private var instance = new TestDispatcher
  def testDispatcher: TestDispatcher = instance

  def createNewInstance() {
    instance = new TestDispatcher
  }

}

