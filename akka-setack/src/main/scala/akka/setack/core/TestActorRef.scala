/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core

import akka.actor._
import java.net.InetSocketAddress
import akka.dispatch.MessageInvocation
import monitor._
import akka.dispatch.ActorPromise
import java.util.concurrent.atomic.AtomicReference
import akka.actor.Props
import com.eaio.uuid.UUID
import akka.dispatch.Future
import scala.collection.mutable.HashSet

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

class TestActorRef(props: Props, address: String, traceMonitorActor: ActorRef) extends LocalActorRef( /*props.withDispatcher(testDispatcher)*/ props, address, false) {
  import MessageEventEnum._

  /**
   * A container for the messges that should be posted to the mailbox later
   */
  @volatile
  private var _cloudMessages = new HashSet[RealMessageInvocation]()

  /**
   * A set of partial orders between the messages. It is used to remove some nondeterminism from the execution.
   * TestSchedule is thread-safe.
   */
  @volatile
  private var _currentSchedule: TestSchedule = null

  /**
   * Callback for the Dispatcher. Informs the monitor actor about processing a message.
   */
  override def invoke(messageHandle: MessageInvocation): Unit = {
    try {
      super.invoke(messageHandle)
    } finally {
      traceMonitorActor ! AsyncMessageEvent(new RealMessageInvocation(messageHandle.receiver, messageHandle.message, messageHandle.channel), MessageEventEnum.Processed)
      log("sent processing" + messageHandle.message)

    }
  }

  /**
   * Overrides the reply method to keep track of the messages sent to the ActorCompletableFutures
   */
  override def reply(message: Any) = {
    if (channel.isInstanceOf[ActorPromise]) {
      traceMonitorActor ! ReplyMessageEvent(new RealMessageInvocation(channel, message, this))
    }
    super.reply(message)
  }

  /**
   *
   * Overrides the tryReply method to keep track of the messages sent to the ActorCompletableFutures
   */
  override def tryReply(message: Any): Boolean = {
    if (channel.isInstanceOf[ActorPromise]) {
      traceMonitorActor ! ReplyMessageEvent(new RealMessageInvocation(channel, message, this))
    }
    super.tryTell(message)
  }

  /**
   * @return reference to the actor object, where the static type matches the factory used inside the
   * constructor. This reference is discarded upon restarting the actor
   */
  def actorObject[T <: Actor]: T = actorInstance.asInstanceOf[AtomicReference[T]].get

  /**
   * Overrides the postMessageToMailbox to apply the constraints in the schedule if there is any
   */
  override protected[akka] def postMessageToMailbox(message: Any, channel: UntypedChannel): Unit = {
    if (_currentSchedule == null) postMessageToMailboxWithoutCheck(message, channel)
    else {
      postMessageBySchedule(message, channel)
    }
  }

  /**
   * Calls the postMessageToMailbox without checking any condition and informs the monitor actor about
   * the delivery of a message
   */
  private def postMessageToMailboxWithoutCheck(message: Any, channel: UntypedChannel): Unit = {
    super.postMessageToMailbox(message, channel)
    traceMonitorActor ! AsyncMessageEvent(new RealMessageInvocation(this, message, channel), Delivered)
  }

  /**
   * It checks the position of the message in the schedule schedule:
   * 1) if the message is not in the schedule then it calls postMessageToMailboxWithoutCheck
   * 2) if the message  or it is in the head of the schedule it calls postMessageToMailboxWithoutCheck and
   * removes the message from the head of the schedule
   * 3) if the message is somewhere in the schedule other than the head, it keeps the message in the cloud
   */
  private def postMessageBySchedule(message: Any, channel: UntypedChannel) = synchronized {
    val invocation = new RealMessageInvocation(this, message, channel)
    log("message index:" + message + " " + _currentSchedule.leastIndexOf(invocation))
    _currentSchedule.leastIndexOf(invocation) match {
      case -1 ⇒ postMessageToMailboxWithoutCheck(message, channel)
      case 0 ⇒ {
        postMessageToMailboxWithoutCheck(message, channel)
        removeFromScheduleAndCheckForDeliveryFromCloud(invocation)
      }
      case _ ⇒ _cloudMessages.add(invocation) //; println("added to cloud" + invocation)
    }

  }

  /**
   * Overrides the postMessageToMailboxAndCreateFutureResultWithTimeout to
   * apply the constraints in the schedule if there is any
   */
  override protected[akka] def postMessageToMailboxAndCreateFutureResultWithTimeout(
    message: Any,
    timeout: Timeout,
    channel: UntypedChannel): Future[Any] = {
    if (_currentSchedule == null) postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck(message, timeout, channel)
    else {
      postMessageAndCreateFutureBySchedule(message, timeout, channel)

    }
  }

  /**
   * Calls the postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck without checking
   * any condition and informs the monitor actor about the delivery of a message
   */
  private def postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck(
    message: Any,
    timeout: Timeout,
    channel: UntypedChannel): Future[Any] = {
    val future = super.postMessageToMailboxAndCreateFutureResultWithTimeout(message, timeout, channel)
    traceMonitorActor ! AsyncMessageEvent(new RealMessageInvocation(this, message, future.asInstanceOf[ActorPromise]), Delivered)
    future
  }

  /**
   * It creates a future for the sender of the invocation
   */
  private def createFuture(
    timeout: Timeout,
    channel: UntypedChannel): Future[Any] = if (isRunning) {
    val future = channel match {
      case f: ActorPromise ⇒ f
      case _               ⇒ new ActorPromise(timeout)(dispatcher)
    }
    future
  } else throw new ActorInitializationException("Actor has not been started, you need to invoke 'actor' before using it")

  /**
   * It checks the position of the message in the schedule schedule:
   * 1) if the message is not in the schedule then it calls postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck
   * 2) if the message  or it is in the head of the schedule it calls postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck,
   * removes the message from the head of the schedule, and returns the future
   * 3) if the message is somewhere in the schedule other than the head, it creates the future, keeps the message in the cloud and
   * returns the future
   */
  private def postMessageAndCreateFutureBySchedule(message: Any, timeout: Timeout, channel: UntypedChannel): Future[Any] = synchronized {

    var invocation = new RealMessageInvocation(this, message, channel)
    log("message index:" + message + " " + _currentSchedule.leastIndexOf(invocation))
    _currentSchedule.leastIndexOf(invocation) match {
      case -1 ⇒ postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck(message, timeout, channel)
      case 0 ⇒ {
        val future = postMessageToMailboxAndCreateFutureResultWithTimeoutWithoutCheck(message, timeout, channel)
        removeFromScheduleAndCheckForDeliveryFromCloud(invocation)
        future
      }
      case _ ⇒ {
        val future = createFuture(timeout, channel)
        invocation = new RealMessageInvocation(this, message, future.asInstanceOf[ActorPromise])
        _cloudMessages.add(invocation)
        log("added to cloud" + invocation)
        future
      }
    }

  }

  /**
   * Removes the delivered message from the head of schedule and checks for the further
   * delivery from the messages in the cloud. This method is synchronized by caller.
   */
  private def removeFromScheduleAndCheckForDeliveryFromCloud(invocation: RealMessageInvocation) {
    var scheduleUpdated = _currentSchedule.removeFromHead(invocation)
    log("removeFromSchedule: " + invocation.message)
    while (scheduleUpdated) {
      scheduleUpdated = checkForDeliveryFromCloud()
    }
  }

  /**
   * Checks if there is any message in the cloud that can be delivered.
   * In the case that there is a message in cloud which is in the head of any partial orders in the
   * schedule, it posts the message into the mailbox,
   * removes the message from the cloud, and updates the schedule (which returns true).
   * In the case that nothing from the cloud can be delivered, it returns false.
   */
  private def checkForDeliveryFromCloud(): Boolean = {
    for (invocation ← _cloudMessages) {
      if (_currentSchedule.leastIndexOf(invocation) == 0) {
        postMessageToMailboxWithoutCheck(invocation.message, invocation.sender)
        _cloudMessages.-=(invocation)
        _currentSchedule.removeFromHead(invocation)
      }
    }
    false

  }

  /**
   * Adds a partial order between the message to the schedule
   */
  def addPartialOrderToSchedule(po: TestMessageInvocationSequence) = synchronized {
    if (_currentSchedule == null) _currentSchedule = new TestSchedule(Set(po))
    else _currentSchedule.addPartialOrder(po)
    log("current schedule= " + _currentSchedule.toString())
  }

  /**
   * It is called by the end of the test to make sure that the specified schedule happened
   */
  def scheduleHappened = synchronized {
    _cloudMessages.isEmpty && _currentSchedule.isEmpty
  }

  private var debug = false
  private def log(s: String) = if (debug) println(s)

}

