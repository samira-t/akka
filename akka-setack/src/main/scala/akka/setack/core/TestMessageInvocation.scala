/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core
import akka.dispatch.MessageInvocation
import scala.collection.mutable.ListBuffer

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
 * The wild card for the sender and receiver is an instance of AnyActorRef, anyActorRef
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
class TestMessageInvocation extends TestMessageInvocationSequence {

  var _sender: TestActorRef = null
  var _receiver: TestActorRef = null
  var _message: Any = null
  var _messagePattern: PartialFunction[Any, Any] = null

  private def this(sender: TestActorRef, receiver: TestActorRef) {
    this()
    this._sender = sender
    this._receiver = receiver
    _messageSequence.+=(this)
  }

  def this(sender: TestActorRef, receiver: TestActorRef, message: Any) {
    this(sender, receiver)
    this._message = message
  }

  def this(sender: TestActorRef, receiver: TestActorRef, messagePattern: PartialFunction[Any, Any]) {
    this(sender, receiver)
    this._messagePattern = messagePattern
  }

  def receiver = _receiver

  def sender = _sender

  def message = _message

  def messagePattern = _messagePattern

  def matchWithRealInvocation(realInvocation: MessageInvocation): Boolean = {
    log("matching " + this.toString() + " " + realInvocation.toString())

    if (!this.receiver.matchesWith(realInvocation.receiver)) { log("receiver false"); return false }

    if (!this.sender.matchesWith(realInvocation.channel)) { log("sender fasle" + realInvocation.channel); return false }

    if (this.message != null && !realInvocation.message.equals(this.message)) { log("message false"); return false }

    if (this.messagePattern != null && !this.messagePattern.isDefinedAt(realInvocation.message))
      return false
    log(" returns true")
    return true
  }

  override def toString(): String = {
    var outString = "(" + sender.toString() + "," + receiver.toString() + ","
    if (message != null) outString += message.toString()
    else outString += messagePattern.toString()
    return outString + ")"
  }

}

/**
 * Each test message invocation sequence is an ordered set of test message
 * invocations. The sequence is defined by using '->' operator.
 *
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
class TestMessageInvocationSequence {

  protected var _messageSequence = new ListBuffer[TestMessageInvocation]

  def ->(testMessage: TestMessageInvocation): TestMessageInvocationSequence = {
    _messageSequence.+=(testMessage)
    return this
  }

  def head: TestMessageInvocation = {
    if (!_messageSequence.isEmpty)
      _messageSequence.head
    else return null
  }

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

  def indexOf(invocation: MessageInvocation): Int = {
    return _messageSequence.findIndexOf(m ⇒ m.matchWithRealInvocation(invocation))
  }

  def log(str: String) {
    //println(str)
  }

}