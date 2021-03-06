/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.setack.test

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.BeforeAndAfterEach

//import akka.testkit._
//import akka.testkit.Testing.sleepFor
//import akka.util.duration._

//import Actor._
import akka.actor.Actor
import akka.config.Supervision._
import akka.dispatch.Dispatchers
import akka.actor.ActorRef
import akka.actor.Props

import akka.setack.util.TestActorRefFactory._
import akka.setack.util.TestMessageUtil._
import akka.setack.util.TestExecutionUtil._
import akka.setack.util.Assert._
import akka.setack.core.TestMessageInvocation
import akka.setack.core.TestActorRef
import akka.setack.SetackWordSpec

object ActorFireForgetRequestReplySpec {

  class ReplyActor extends Actor {
    def receive = {
      case "Send" ⇒
        self.reply("Reply")
      case "SendImplicit" ⇒
        self.channel ! "ReplyImplicit"
    }
  }

  class CrashingActor extends Actor {
    def receive = {
      case "Die" ⇒
        //state.finished.await
        throw new Exception("Expected exception")
    }
  }

  class SenderActor(replyActor: ActorRef) extends Actor {
    def receive = {
      case "Init" ⇒
        replyActor ! "Send"
      case "Reply" ⇒ {
        //state.s = "Reply"
        //state.finished.await
      }
      case "InitImplicit" ⇒ replyActor ! "SendImplicit"
      case "ReplyImplicit" ⇒ {
        //state.s = "ReplyImplicit"
        //state.finished.await
      }
    }
  }

  //  object state {
  //    var s = "NIL"
  //    val finished = TestBarrier(2)
  //  }
}

class ActorFireForgetRequestReplySpec extends SetackWordSpec with MustMatchers { //with BeforeAndAfterEach {
  import ActorFireForgetRequestReplySpec._

  //  override def beforeEach() = {
  //    state.finished.reset
  //  }

  "An Actor" must {

    "reply to bang message using reply" in {
      val replyActor = actorOf[ReplyActor]
      val senderActor = actorOf(new SenderActor(replyActor))

      /* Added by Setack*/
      val replyMsg = testMessage(anyActorRef, senderActor, "Reply")

      senderActor ! "Init"

      //state.finished.await
      //state.s must be("Reply")

      /* Added by Setack*/
      whenStable {
        isProcessed(replyMsg) must be(true)
      }
    }

    "reply to bang message using implicit sender" in {
      val replyActor = actorOf[ReplyActor]
      val senderActor = actorOf(new SenderActor(replyActor))

      /* Added by Setack*/
      val replyImplicit = testMessage(anyActorRef, senderActor, "ReplyImplicit")

      senderActor ! "InitImplicit"

      //state.finished.await
      //state.s must be("ReplyImplicit")

      whenStable {
        isProcessed(replyImplicit) must be(true)
      }
    }

    "should shutdown crashed temporary actor" in {
      //filterEvents(EventFilter[Exception]("Expected")) {
      val actor = actorOf(Props[CrashingActor].withLifeCycle(Temporary))
      actor.isRunning must be(true)
      try {
        actor ! "Die"
      } catch {
        case ex ⇒
      }

      //state.finished.await
      //sleepFor(1 second)

      whenStable {
        actor.isShutdown must be(true)
      }
      //}
    }
  }
}
