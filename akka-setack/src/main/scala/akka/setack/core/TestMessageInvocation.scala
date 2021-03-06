/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core
import akka.dispatch.MessageInvocation
import scala.collection.mutable.ListBuffer
import akka.actor.UntypedChannel
import akka.setack.util.TestActorRefFactory

class RealMessageInvocation(_reciever: UntypedChannel, _message: Any, _sender: UntypedChannel) {
  def receiver = _reciever
  def message = _message
  def sender = _sender

  def ==(otherInvocation: RealMessageInvocation): Boolean =
    (receiver == otherInvocation.receiver) && (sender == otherInvocation.sender) && (message == otherInvocation.message)
}

/**
 * This enumeration defines two different kinds of the events for the
 * messages:
 * delivered(in the mailbox), processed (removed from the mail box and executed)
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
object MessageEventEnum extends Enumeration {
  type MessageEventType = Value
  val Delivered, Processed = Value
}

/**
 * Each test message invocation is a message defined by the user and
 * can be matched with real messages during the execution.
 *
 * The message property in the test message invocation
 * can be an object or a pattern (partial function)
 *
 * The wild card for the sender and receiver is anyActorRef
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
class TestMessageInvocation extends TestMessageInvocationSequence {

  var _receiver: UntypedChannel = null
  var _sender: UntypedChannel = null
  var _message: Any = null
  var _messagePattern: PartialFunction[Any, Any] = null

  private def this(sender: UntypedChannel, receiver: UntypedChannel) {
    this()
    this._sender = sender
    this._receiver = receiver
    _messageSequence.+=(this)
  }

  def this(sender: UntypedChannel, receiver: UntypedChannel, message: Any) {
    this(sender, receiver)
    this._message = message
  }

  def this(sender: UntypedChannel, receiver: UntypedChannel, messagePattern: PartialFunction[Any, Any]) {
    this(sender, receiver)
    this._messagePattern = messagePattern
  }

  def receiver = _receiver

  def sender = _sender

  def message = _message

  def messagePattern = _messagePattern

  def matchWithRealInvocation(realInvocation: RealMessageInvocation): Boolean = {
    log("matching " + this.toString() + " " + realInvocation.toString())
    if (!compareChannels(this.sender, realInvocation.sender)) return false

    if (!compareChannels(this.receiver, realInvocation.receiver)) return false

    if (this.message != null && realInvocation.message != this.message) { log("message false"); return false }

    if (this.messagePattern != null && !this.messagePattern.isDefinedAt(realInvocation.message)) return false
    log(" returns true")
    return true
  }

  private def compareChannels(ch1: UntypedChannel, ch2: UntypedChannel): Boolean = {
    (ch1.isInstanceOf[TestActorRef] && ch1 == TestActorRefFactory.anyActorRef) ||
      (ch2.isInstanceOf[TestActorRef] && ch2 == TestActorRefFactory.anyActorRef) ||
      (ch1 == ch2)
  }

  override def toString(): String = "(" + sender + "," + receiver + "," + (if (message != null) message else messagePattern) + ")"

  //only for debugging
  private var debug = false
  private def log(s: String) = if (debug) println(s)

}

/**
 * Each test message invocation sequence is an ordered set of test message
 * invocations. The sequence is defined by using '->' operator.
 * The assumption is that the sender of all messages in a given sequence are the same.
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
class TestMessageInvocationSequence {

  protected var _messageSequence = new ListBuffer[TestMessageInvocation]

  def ->(testMessage: TestMessageInvocation): TestMessageInvocationSequence = {
    _messageSequence.+=(testMessage)
    return this
  }

  def head: TestMessageInvocation = _messageSequence.headOption.orNull

  def removeHead: Boolean = {
    if (!_messageSequence.isEmpty) {
      _messageSequence.-=(_messageSequence.head)
      return true
    }
    return false
  }

  def messageSequence = _messageSequence

  def isEmpty: Boolean = {
    return _messageSequence.isEmpty
  }

  def indexOf(invocation: RealMessageInvocation): Int = {
    return _messageSequence.findIndexOf(m ⇒ m.matchWithRealInvocation(invocation))
  }

  def equals(otherSequence: TestMessageInvocationSequence): Boolean = {
    for (msg ← _messageSequence) {
      if (!otherSequence.head.equals(msg)) return false
    }
    return true
  }

}