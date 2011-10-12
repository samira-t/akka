/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core

import akka.actor._
import java.net.InetSocketAddress
import akka.dispatch.MessageInvocation
import monitor._
import dispatcher._
import akka.dispatch.ActorPromise
import java.util.concurrent.atomic.AtomicReference
import akka.actor.Props
import com.eaio.uuid.UUID
import akka.setack.util.TestActorRef._
import TestDispatcher._

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */

class TestActorRef(props: Props, address: String) extends LocalActorRef(props.withDispatcher(testDispatcher), address, false) {
  import MessageEventEnum._

  override def restart(reason: Throwable, maxNrOfRetries: Option[Int], withinTimeRange: Option[Int]) {
    super.restart(reason, maxNrOfRetries, withinTimeRange)
  }

  /**
   * Callback for the TestDispatcher. This is the single entry point to the user Actor implementation.
   */
  override def invoke(messageHandle: MessageInvocation): Unit = {
    try {
      super.invoke(messageHandle)
    } catch {
      case e ⇒ {
        throw e
      }
    } finally {
      Monitor.traceMonitorActor ! AsyncMessageEvent(messageHandle, MessageEventEnum.Processed)

    }
  }

  /**
   * Overrides the reply method to keep track of the messages sent to the ActorCompletableFutures
   */
  override def reply(message: Any) = {
    if (channel.isInstanceOf[ActorPromise]) {
      Monitor.traceMonitorActor ! ReplyMessageEvent(new MessageInvocation(akka.setack.util.TestActorRef.anyActorRef, message, this))
    }
    super.reply(message)
    //channel.!(message)(this)
  }

  /**
   *
   * Overrides the tryReply method to keep track of the messages sent to the ActorCompletableFutures
   */
  override def tryReply(message: Any): Boolean = {
    if (channel.isInstanceOf[ActorPromise]) {
      Monitor.traceMonitorActor ! ReplyMessageEvent(new MessageInvocation(akka.setack.util.TestActorRef.anyActorRef, message, this))
    }
    super.tryTell(message)
    //channel.tryTell(message)(this)
  }

  /**
   * @return reference to the actor object, where the static type matches the factory used inside the
   * constructor. This reference is discarded upon restarting the actor
   */
  @volatile
  def actorObject[T <: Actor]: T = actorInstance.asInstanceOf[AtomicReference[T]].get

  /**
   * Checks if the sender or receiver of the real invocation and
   * the test message are matched
   *
   * Any instance of AnyActor will be matched with any other sender or receiver
   */

  def matchesWith(otherRef: UntypedChannel): Boolean =
    {
      //println(this.actorInstance.getClass + (this == TestActorRefFactory.anyActorRef).toString)

      val anyRef = akka.setack.util.TestActorRef.anyActorRef
      if (this == anyActorRef || otherRef == anyRef) return true
      if (otherRef.isInstanceOf[ActorRef]) return (this.uuid == otherRef.asInstanceOf[ActorRef].uuid)
      else return false
    }

}

