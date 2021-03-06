/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.test
import akka.actor.Actor
import akka.actor.ActorRef
import akka.setack.util.TestActorRefFactory._
import akka.setack.util.TestMessageUtil._
import akka.setack.util.TestExecutionUtil._
import akka.setack.util.Assert._
import org.junit.Test
import org.junit.Before
import org.junit.After
import akka.setack.core.TestMessageInvocation
import akka.setack.core.TestActorRef
import akka.setack._
import scala.collection.mutable.ListBuffer

class SampleActor(var brother: ActorRef = null) extends Actor {
  var messageOrder = ListBuffer[Any]()
  def receive = {
    case msg @ ('m)  ⇒ messageOrder.+=(msg)
    case msg @ 'req  ⇒ messageOrder.+=(msg); if (brother != null) { val f = brother ? 'req2; f.get }
    case msg @ 'req2 ⇒ messageOrder.+=(msg); self.reply('reply)

  }
}

class TestFutureMeesages extends SetackJUnit with org.scalatest.junit.JUnitSuite {

  var a: TestActorRef = null
  var b: TestActorRef = null
  var m: TestMessageInvocation = null
  var req2: TestMessageInvocation = null
  var reply: TestMessageInvocation = null

  @Before
  def setUp {
    a = actorOf(new SampleActor())
    b = actorOf(new SampleActor(a))
    m = testMessage(anyActorRef, a, 'm)
    req2 = testMessage(anyActorRef, a, 'req2)
    reply = testMessage(a, anyActorRef, 'reply)
  }

  @Test
  def testDeliveryToFuture {
    b ! 'req

    whenStable {
      assert(isProcessed(req2))
      assert(isDelivered(reply))
    }

  }

  @Test
  def testScheduleFutureAndActorMessages {
    setSchedule(req2 -> m)
    a ! 'm
    b ! 'req

    whenStable {
      assert(isProcessed(req2))
      assert(isDelivered(reply))
      assert(a.actorObject[SampleActor].messageOrder.indexOf('req2) < a.actorObject[SampleActor].messageOrder.indexOf('m))
    }
  }

  @After
  def tearDown {
  }

}