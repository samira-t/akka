package akka.actor

import akka.testkit.TestKit
import akka.util.duration._

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class FSMInitSpec extends WordSpec with MustMatchers with TestKit {

  trait Queryable extends Actor with FSM[Int, Int] {
    def current = stay
  }

  trait EarlyQ extends Queryable {
    when(1) {
      case _ => stay replying 1
    }
    when(2) {
      case _ => stay replying 2
    }
  }

  class Early extends EarlyQ {
    startWith(1, 1)
  }

  class LateQ extends Queryable with LateInit {
    when(1) {
      case _ => stay replying 1
    }
    when(2) {
      case _ => stay replying 2
    }

    override def preStart {
      super.preStart
    }
  }

  class Late extends LateQ {
    startWith(2, 2)
  }

  "An FSM" should {
    "support early initialization while subclassing" in {
      lazy val fsm = new Early
      val fsmref = Actor.actorOf(fsm)
      fsm.current must be (fsm.State(1, 1, None))
      fsmref.start
      within(50 millis) {
        fsmref ! 1
        expectMsg(1)
      }
      fsmref.stop
    }

    "support late initialization" in {
      lazy val fsm = new Late
      val fsmref = Actor.actorOf(fsm)
      evaluating { fsm.current } must produce [NullPointerException]
      fsmref.start
      fsm.current must be (fsm.State(2, 2, None))
      within(50 millis) {
        fsmref ! 1
        expectMsg(2)
      }
      fsmref.stop
    }
  }

}

// vim: set ts=2 sw=2 et:
