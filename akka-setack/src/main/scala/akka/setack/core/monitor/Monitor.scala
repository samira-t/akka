/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core.monitor
import akka.actor.Actor
import akka.dispatch.MessageInvocation
import scala.collection.mutable.ListBuffer
import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import akka.setack.core.TestMessageInvocation
import akka.setack.core.RealMessageInvocation
import akka.setack.core.MessageEventEnum._
import akka.actor.LocalActorRef

abstract class MonitorActorMessage
case class AsyncMessageEvent(message: RealMessageInvocation, event: MessageEventType) extends MonitorActorMessage
case class ReplyMessageEvent(message: RealMessageInvocation) extends MonitorActorMessage
case class MatchedMessageEventCount(testMessage: TestMessageInvocation, event: MessageEventType) extends MonitorActorMessage
case class AddTestMessage(testMessage: TestMessageInvocation) extends MonitorActorMessage
case object AllDeliveredMessagesAreProcessed extends MonitorActorMessage
case object NotProcessedMessages extends MonitorActorMessage

/**
 * This object manages the monitoring of the test execution.
 * It contains the set of test message invocations defined by the user
 * and an actor that uses this set to receive the messages sent by TestActorRef
 * regarding to delivery and processing of the messages.
 * For the efficiency, the actor just keeps track of the messages that match
 * with the user defined test messages.
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
class Monitor {

  @volatile
  var traceMonitorActor: ActorRef = null

  private var definedTestMessages = new HashSet[TestMessageInvocation]

  /**
   * This method is called by the TestMessageInvocationFactory when the user
   * creates and defines a test message.
   */
  def addTestMessage(testMessageInvocation: TestMessageInvocation) {
    definedTestMessages.add(testMessageInvocation)
    /*
     * If the test is already running, add the new test message to the list of the messages in the monitoring actor
     */
    if (traceMonitorActor != null && traceMonitorActor.isRunning) {
      val f = (traceMonitorActor ? AddTestMessage(testMessageInvocation))
      f.get
    }
  }

  /**
   * It is called before each test starts.It starts a new monitoring actor.
   */
  def startMonitoring() {
    traceMonitorActor = Actor.actorOf(new TraceMonitorActor(definedTestMessages))
  }

  /**
   * It is called after each test. It clears its local state and stops the monitoring actor
   */
  def stopMonitoring() {
    traceMonitorActor.stop
    definedTestMessages.clear()
  }

}

class TraceMonitorActor(definedTestMessages: HashSet[TestMessageInvocation]) extends Actor {

  var testMessagesInfo = new HashMap[TestMessageInvocation, Array[Int]]()
  var deliveredAsyncMessages = new ArrayBuffer[RealMessageInvocation]()
  var messageTrace = new ListBuffer[TestMessageInvocation]

  override def preStart() {
    for (testMessage ← definedTestMessages) {
      testMessagesInfo.put(testMessage, Array(0, 0))
    }
  }

  def receive =
    {
      case AddTestMessage(testMessage) ⇒ {
        testMessagesInfo.put(testMessage, Array(0, 0))
        self.reply()
      }
      case AsyncMessageEvent(message, event) ⇒ {
        val matchedTestMessages = testMessagesInfo.filterKeys(m ⇒ m.matchWithRealInvocation(message))
        for ((testMsg, dp) ← matchedTestMessages) {
          event match {
            case Delivered ⇒ {
              deliveredAsyncMessages.+=(message)
              testMessagesInfo.update(testMsg, Array(dp(0) + 1, dp(1)))
            }
            case Processed ⇒ {
              testMessagesInfo.update(testMsg, Array(dp(0), dp(1) + 1))
              val index = deliveredAsyncMessages.indexWhere(m ⇒ m == message)
              if (index >= 0) deliveredAsyncMessages.remove(index)
              if (message.message.equals("Reply")) log("reply" + index + " " + testMessagesInfo(testMsg)(1))
              log("received processing: " + message.message + " " + message.receiver)
            }

          }
        }
      }
      case ReplyMessageEvent(message) ⇒ {
        //        messageTrace.+=(message)
        val matchedTestMessages = testMessagesInfo.filterKeys(m ⇒ m.matchWithRealInvocation(message))
        for ((testMsg, dp) ← matchedTestMessages) {
          testMessagesInfo.update(testMsg, Array(dp(0) + 1, dp(1) + 1))
        }

      }
      /**
       * returns the set of the real  messages that are matched with the test message and the specified event
       */
      case MatchedMessageEventCount(testMessage, event) ⇒ {
        event match {
          case Delivered ⇒ self.reply(testMessagesInfo(testMessage)(0))
          case Processed ⇒ {
            self.reply(testMessagesInfo(testMessage)(1))
            log(testMessage.message + " " + testMessagesInfo(testMessage)(1) + " " + testMessagesInfo(testMessage)(0))
          }
        }
      }
      case AllDeliveredMessagesAreProcessed ⇒ self.reply(deliveredAsyncMessages.size == 0)
      case NotProcessedMessages             ⇒ self.reply(deliveredAsyncMessages)

    }

  //for debugging only
  private var debug = false
  private def log(s: String) = if (debug) println(s)

}
