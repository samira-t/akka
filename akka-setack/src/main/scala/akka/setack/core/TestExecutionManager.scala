/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.setack.core
import akka.actor.Actor
import akka.actor.ActorRef
import monitor.Monitor.traceMonitorActor
import monitor.NotProcessedMessages
import monitor.AllDeliveredMessagesAreProcessed
import akka.dispatch._
import akka.actor.LocalActorRef
import akka.setack.core.dispatcher.TestDispatcher
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

/**
 * @author <a href="http://www.cs.illinois.edu/homes/tasharo1">Samira Tasharofi</a>
 */
object TestExecutionManager {

  private var runningTestActors = new HashSet[ActorRef]

  /**
   * Starts the monitor actor
   */
  def startTest {
    monitor.Monitor.startMonitoring()
  }

  /**
   * Checks for the stability of the system.
   * The system is stable if:
   * 1) the mailbox of the actors in the test are empty (all the actors have processed their messages)
   * 2) the mailbox of the monitor actor is empty (the monitor actor has processed all of his notifications)
   * 3) the dispatcher has finished the schedule and dispatched all the messages in the cloud
   *
   * It tries to check the stability of the system by giving maxTry. the default is 10 times
   * and each time it sleeps 100 milliseconds before trying again.
   *
   * @return false if the system did not get stable with that maxTry
   *
   *
   */
  def checkForStability(maxTry: Int = 10): Boolean =
    {
      var triedCheck = 0
      var isStable = false
      val testActors = Actor.registry.local.filter(a ⇒ a.isInstanceOf[TestActorRef])
      while (triedCheck < maxTry) {
        if (triedCheck > 0)
          Thread.sleep(100)
        triedCheck += 1

        var someIsRunning = false
        runningTestActors.clear()

        /*        for (a ← testActors) {

          if (a.isRunning && !TestDispatcher.mailboxIsEmpty(a.asInstanceOf[LocalActorRef])) {
            log(a + " has message")
            if (triedCheck == maxTry)
              runningTestActors.add(a)
            someIsRunning = true
          }
        }
*/ for (a ← Actor.registry.local) {

          if (a.isRunning && !a.asInstanceOf[LocalActorRef].dispatcher.mailboxIsEmpty(a.asInstanceOf[LocalActorRef])) {
            log(a + " has message")
            if (triedCheck == maxTry)
              runningTestActors.add(a)
            someIsRunning = true
          }
        }

        if (!someIsRunning) {
          if (!TestDispatcher.isFinished) {
            someIsRunning = true
          } else if (traceMonitorActor.isRunning &&
            !traceMonitorActor.asInstanceOf[LocalActorRef].dispatcher.mailboxIsEmpty(traceMonitorActor.asInstanceOf[LocalActorRef])) {
            someIsRunning = true
          } else if (!(traceMonitorActor ? AllDeliveredMessagesAreProcessed).get.asInstanceOf[Boolean]) {
            someIsRunning = true
            if (triedCheck == maxTry) {
              val notProcessedMessages = (traceMonitorActor ? NotProcessedMessages).get.asInstanceOf[ArrayBuffer[MessageInvocation]]
              notProcessedMessages.foreach(m ⇒ runningTestActors.add(m.receiver.asInstanceOf[ActorRef]))
            }
            log("not all delivered messages are processed yet")

          }
        }
        if (!someIsRunning) return true
      }
      return false

    }

  /**
   * Stops the test by stopping all the test actors and the monitor actor
   */
  def stopTest {
    log("stability for stopping the test")
    val stable = checkForStability()
    for (actor ← runningTestActors) {
      log("unregister" + actor)
      Actor.registry.unregister(actor)
      log(actor + " is running and cannot be stopped")
    }
    Actor.registry.local.shutdownAll
    monitor.Monitor.stopMonitoring()
    TestDispatcher.clearState()
    if (!stable) {
      log(" The system did not get stable after the test, tear down")

    }

  }
  private def log(msg: String) {
    //println(msg)
  }
}