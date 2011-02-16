/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */
package akka.actor.supervisor

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor._
import akka.config.Supervision._

import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.matchers.MustMatchers

class Ticket669Spec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  import Ticket669Spec._

  override def afterAll = Actor.registry.shutdownAll

  "An supervised actor" should {
    "be able to reply during pre-restart" in {
      val latch = new CountDownLatch(1)
      val sender = Actor.actorOf(new Sender(latch)).start

      val supervised = Actor.actorOf[Supervised]
      val supervisor = Supervisor(SupervisorConfig(
        AllForOneStrategy(List(classOf[Exception]), 5, 10000),
        Supervise(supervised, Permanent) :: Nil)
      )

      supervised.!("not ok")(Some(sender))
      latch.await(5, TimeUnit.SECONDS) must be (true)
    }
  }
}

object Ticket669Spec {
  class Sender(latch: CountDownLatch) extends Actor {
    def receive = {
      case "failed" => latch.countDown
      case _        => { }
    }
  }

  class Supervised extends Actor {
    def receive = {
      case msg => if (msg == "ok") self.reply("done") else throw new Exception("test")
    }

    override def preRestart(reason: scala.Throwable) {
      self.reply("failed")
    }
  }
}