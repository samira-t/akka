/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.test

import akka.setack._
import akka.setack.util.TestMessageUtil._
import akka.setack.util.TestExecutionUtil._
import akka.setack.util.Assert._
import akka.setack.util.TestActorRefFactory._
import org.junit.Test
import org.junit.Before
import org.junit.After
import akka.setack.core.TestMessageInvocation
import akka.setack.core.TestActorRef
import akka.actor.Actor
import scala.collection.mutable.HashSet

case class IntMessage(x: Int)
case class BooleanMessage(b: Boolean)

class IntBoolActor extends Actor {
  var intMessages = new HashSet[Int]
  var boolMessages = new HashSet[Boolean]
  def receive = {
    case IntMessage(x)     ⇒ intMessages.add(x)
    case BooleanMessage(b) ⇒ boolMessages.add(b)
  }
}

class JUnitTestMessageMatching extends SetackJUnit with org.scalatest.junit.JUnitSuite {

  var a: TestActorRef = null
  var intAny: TestMessageInvocation = null
  var int1: TestMessageInvocation = null
  var btrue: TestMessageInvocation = null
  var bAny: TestMessageInvocation = null

  @Before
  def setUp {
    a = actorOf(new IntBoolActor())
    int1 = testMessage(anyActorRef, a, IntMessage(1))
    intAny = testMessagePattern(anyActorRef, a, { case IntMessage(_) ⇒ })
    btrue = testMessage(anyActorRef, a, BooleanMessage(true))
    bAny = testMessagePattern(anyActorRef, a, { case BooleanMessage(_) ⇒ })
  }

  @Test
  def test1 {
    a ! IntMessage(5)
    a ! IntMessage(1)
    a ! BooleanMessage(false)
    a ! BooleanMessage(false)

    whenStable {
      assert(isDelivered(int1))
      assert(isProcessed(intAny))
      assert(deliveryCount(intAny) == 2)
      assert(processingCount(bAny) == 2)
    }
  }

  @After
  def tearDown {
  }

}

class ScalaTestMessageMatching extends SetackFlatSpec with org.scalatest.matchers.ShouldMatchers {

  var testActor: TestActorRef = null
  var intAny: TestMessageInvocation = null
  var int1: TestMessageInvocation = null
  var btrue: TestMessageInvocation = null
  var bAny: TestMessageInvocation = null

  override def setUp {
    testActor = actorOf(new IntBoolActor())
    int1 = testMessage(anyActorRef, testActor, IntMessage(1))
    intAny = testMessagePattern(anyActorRef, testActor, { case IntMessage(_) ⇒ })
    btrue = testMessage(anyActorRef, testActor, BooleanMessage(true))
    bAny = testMessagePattern(anyActorRef, testActor, { case BooleanMessage(_) ⇒ })
  }

  "The Int and Bool messages" should "be processed" in {
    testActor ! IntMessage(5)
    testActor ! IntMessage(1)
    testActor ! BooleanMessage(false)
    testActor ! BooleanMessage(false)

    whenStable {
      processingCount(bAny) should be(2)
      isDelivered(int1) should be(true)
      isProcessed(intAny) should be(true)
      deliveryCount(intAny) should be(2)
      processingCount(bAny) should be(2)
    }

  }

  override def tearDown {
  }

}